import os
import requests
import pandas as pd
from datetime import datetime
from sqlalchemy import create_engine, types, text
from dotenv import load_dotenv
import psycopg2
from psycopg2.extras import execute_values

load_dotenv()

# ---------------------------
# Environment variables setup
# ---------------------------
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
API_KEY = os.getenv("OPENWEATHERMAP_API_KEY")  # Your OpenWeatherMap API key

TABLE_NAME = "meteo_forecast"
# ---------------------------

# Base abstract class for a Meteo wrapper
class MeteoWrapper:
    def load_data(self, location):
        """Fetch the raw weather data from the source."""
        raise NotImplementedError("Subclasses must implement load_data()")
    
    def parse_data(self, raw_data):
        """Parse and normalize the raw weather data into a DataFrame."""
        raise NotImplementedError("Subclasses must implement parse_data()")
    
    def get_forecast(self, location):
        raw_data = self.load_data(location)
        return self.parse_data(raw_data)

# Concrete implementation using OpenWeatherMap API
class OpenWeatherMapWrapper(MeteoWrapper):
    BASE_URL = "https://api.openweathermap.org/data/2.5/forecast"
    
    def __init__(self, api_key):
        self.api_key = api_key
        
    def load_data(self, location):
        params = {
            "appid": self.api_key,
            "units": "metric"  # Use metric units (temperature in Celsius, wind speed in m/s)
        }
        # Accept location as a city name (e.g., "Marseille,FR") or (lat,lon) tuple.
        if isinstance(location, str):
            params["q"] = location
        elif isinstance(location, tuple) and len(location) == 2:
            params["lat"] = location[0]
            params["lon"] = location[1]
        else:
            raise ValueError("location must be a string (city name) or a (lat, lon) tuple")
        
        response = requests.get(self.BASE_URL, params=params)
        response.raise_for_status()   # Raise an error if the request failed
        return response.json()
    
    def parse_data(self, raw_data):
        # Normalize the API's JSON output into a standardized DataFrame.
        # Our goal is to output a DataFrame with the following columns:
        # timestamp, temperature, humidity, wind_speed, weather_description
        records = []
        for entry in raw_data.get("list", []):
            dt = datetime.utcfromtimestamp(entry.get("dt"))
            temp = entry["main"]["temp"]
            humidity = entry["main"]["humidity"]
            wind_speed = entry["wind"]["speed"]
            # There may be several weather entries; we take the first one.
            description = entry["weather"][0]["description"]
            records.append({
                "timestamp": dt,
                "temperature": temp,
                "humidity": humidity,
                "wind_speed": wind_speed,
                "weather_description": description
            })
        return pd.DataFrame(records)

def inject_meteo_to_postgres(forecast_df, table_name):
    conn = psycopg2.connect(
        host=DB_HOST,
        port=DB_PORT,
        dbname=DB_NAME,
        user=DB_USER,
        password=DB_PASSWORD
    )
    cursor = conn.cursor()

    # Create table if it doesn't exist
    create_table_sql = f"""
    CREATE TABLE IF NOT EXISTS {table_name} (
        timestamp TIMESTAMP PRIMARY KEY,
        temperature NUMERIC(5,2),
        humidity INTEGER,
        wind_speed NUMERIC(5,2),
        weather_description TEXT
    );
    """
    cursor.execute(create_table_sql)
    conn.commit()

    # 🧹 Prune old forecasts
    today = datetime.utcnow().date()
    delete_sql = f"DELETE FROM {table_name} WHERE timestamp::date < %s;"
    cursor.execute(delete_sql, (today,))
    print(f"🧹 Old forecasts deleted (before {today}).")
    conn.commit()

    # Prepare values
    values = [
        (
            row["timestamp"],
            row["temperature"],
            row["humidity"],
            row["wind_speed"],
            row["weather_description"]
        )
        for _, row in forecast_df.iterrows()
    ]

    insert_sql = f"""
    INSERT INTO {table_name} (
        timestamp, temperature, humidity, wind_speed, weather_description
    ) VALUES %s
    ON CONFLICT (timestamp) DO NOTHING;
    """

    print(f"📤 Inserting {len(values)} rows (skipping duplicates) into '{table_name}'...")
    execute_values(cursor, insert_sql, values)
    conn.commit()
    print(f"✅ Meteo data inserted with pruning and conflict handling.")
    cursor.close()
    conn.close()
    
if __name__ == "__main__":
    # Check that the API key is provided
    if not API_KEY:
        raise ValueError("OPENWEATHERMAP_API_KEY not set in the environment!")
    else:
        print("🔑 OpenWeatherMap API key found.")
        print(f"API Key: {API_KEY}")
    
    # Instantiate the wrapper for OpenWeatherMap
    meteo_wrapper = OpenWeatherMapWrapper(API_KEY)
    
    # Retrieve the forecast data for a specified location (e.g., Marseille,FR)
    forecast_df = meteo_wrapper.get_forecast("Marseille,FR")
    print("Sample meteo forecast data:")
    print(forecast_df.head())
    
    # Insert the normalized forecast data into the PostgreSQL table
    inject_meteo_to_postgres(forecast_df, TABLE_NAME)

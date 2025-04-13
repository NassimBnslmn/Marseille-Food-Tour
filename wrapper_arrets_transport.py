import pandas as pd
import psycopg2
from sqlalchemy import create_engine, types, text
from dotenv import load_dotenv
import os
load_dotenv()

# 🗂️ Environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

TABLE_NAME = "arrets_transport"
CSV_PATH = "cleaned_arrets.csv"

# 🧱 Define column types (all column names will be in lowercase)
column_types = {
    "id": types.Text(),
    "name": types.Text(),
    "latitude": types.Numeric(9, 6),
    "longitude": types.Numeric(9, 6),
    "description": types.Text(),
    "wheelchair boarding": types.Integer()
}

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
        
        print(f"📄 Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path)
        
        # Convert all column names to lowercase (strip whitespace as well)
        df.columns = df.columns.str.strip().str.lower()
        
        # Manipulate the 'coordinates' column:
        # Assume the CSV initially has a column "coordinates" (or adjust if it's spelled differently)
        # Split the coordinates into "latitude" and "longitude"
        if "coordinates" in df.columns:
            print("🛠️ Splitting 'coordinates' into 'latitude' and 'longitude'...")
            df[['latitude', 'longitude']] = df['coordinates'].str.split(',', expand=True)
            df['latitude'] = df['latitude'].astype(float)
            df['longitude'] = df['longitude'].astype(float)
            df = df.drop(columns=['coordinates'])
        else:
            print("ℹ️ No 'coordinates' column found. Check your CSV format.")
        
        print(f"📤 Inserting data into table '{table_name}' with explicit types...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)
        print(f"✅ Data successfully inserted into table '{table_name}'.")
        
    except Exception as e:
        print(f"❌ Error during injection: {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

    # 🔎 Quick SELECT test
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f'SELECT id, name, latitude, longitude, description, "wheelchair boarding" FROM {TABLE_NAME} LIMIT 5;')
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"❌ Error during selection: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("✅ Connection closed.")

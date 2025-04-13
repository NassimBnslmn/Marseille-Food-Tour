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

TABLE_NAME = "evenements_musicaux"
CSV_PATH = "evenement_musical_cleaned.csv"

# 🎼 Define column types
column_types = {
    "titre": types.Text(),
    "description": types.Text(),
    "nom_lieu": types.Text(),
    "adresse": types.Text(),
    "ville": types.Text(),
    "code_postal": types.Integer(),
    "latitude": types.Numeric(9, 6),
    "longitude": types.Numeric(9, 6),
    "date_debut": types.DateTime(),
    "date_fin": types.DateTime()
}

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )

        print(f"📄 Reading CSV file: {csv_path}")
        df = pd.read_csv(csv_path, parse_dates=["date_debut", "date_fin"])

        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        print("Detected columns:", df.columns.tolist())

        df["code_postal"] = pd.to_numeric(df["code_postal"], errors="coerce").astype("Int64")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")

        print(f"📤 Inserting data into '{table_name}'...")
        df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=column_types)
        print(f"✅ Data inserted into '{table_name}'.")

        # Add geography column (instead of geometry)
        with engine.begin() as connection:
            # connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            connection.execute(text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS geog geography(Point, 4326);
            """))
            connection.execute(text(f"""
                UPDATE {table_name}
                SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
            """))
            print("🧭 Geography column 'geog' added and populated.")

    except Exception as e:
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 1;")
        for row in cursor.fetchall():
            print(row)
    except Exception as e:
        print(f"❌ Error during test SELECT: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("✅ Connection closed.")

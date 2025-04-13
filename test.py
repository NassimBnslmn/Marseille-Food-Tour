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

try:
    engine = create_engine(
        f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
    )

    with engine.begin() as connection:
        connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
        connection.execute(text(f"""
            ALTER TABLE {TABLE_NAME}
            ADD COLUMN IF NOT EXISTS geog geography(Point, 4326);
        """))
        connection.execute(text(f"""
            UPDATE {TABLE_NAME}
            SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
            WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
        """))

        print("🧭 Geography column 'geog' added and populated.")

except Exception as e:
    print(f"❌ Error: {e}")


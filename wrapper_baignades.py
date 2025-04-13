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

TABLE_NAME = "baignades"
CSV_PATH = "baignades.csv"  # Path to your CSV file

# Define column types using lower-case names and underscores
column_types = {
    "nom_du_site": types.Text(),
    "categorie": types.Text(),
    "baignade_surveillee": types.Boolean(),
    "adresse": types.Text(),
    "code_postal": types.Integer(),
    "ville": types.Text(),
    "numero_de_telephone": types.Text(),
    "longitude": types.Numeric(9, 6),
    "latitude": types.Numeric(9, 6)
}

def inject_csv_to_postgres(csv_path, table_name):
    try:
        # Create the SQLAlchemy engine
        engine = create_engine(
            f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}'
        )
        
        print(f"📄 Reading CSV file: {csv_path}")
        # Read CSV file with comma delimiter (adjust if needed)
        df = pd.read_csv(csv_path, sep=",")
        
        # Normalize and clean column names: lower-case and replace spaces with underscores
        df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
        print("Detected columns:", df.columns.tolist())
        
        # Convert "baignade_surveillee" to boolean ("oui" => True, "non"/"non_concerne" => False)
        df["baignade_surveillee"] = df["baignade_surveillee"].str.strip().str.lower().map({
            "oui": True,
            "non": False,
            "non concerne": False
        })
        
        # Convert numeric fields
        df["code_postal"] = pd.to_numeric(df["code_postal"], errors="coerce").astype("Int64")
        df["longitude"] = pd.to_numeric(df["longitude"], errors="coerce")
        df["latitude"] = pd.to_numeric(df["latitude"], errors="coerce")
        
        print(f"📤 Inserting data into table '{table_name}' with explicit types...")
        df.to_sql(table_name, engine, if_exists="replace", index=False, dtype=column_types)
        print(f"✅ Data inserted successfully into table '{table_name}'.")
        
        # # ➕ Add geometry column to the table (PostGIS)
        with engine.begin() as connection:
            print("🧩 Activating PostGIS extension...")
            connection.execute(text("CREATE EXTENSION IF NOT EXISTS postgis;"))
            
            print("🧭 Adding geometry column 'geom'...")
            connection.execute(text(f"""
                ALTER TABLE {table_name}
                ADD COLUMN IF NOT EXISTS geom geometry(Point, 4326);
            """))
            
            print("📍 Populating 'geom' column from longitude and latitude...")
            connection.execute(text(f"""
                UPDATE {table_name}
                SET geom = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
                WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
            """))
        
        print(f"✅ Table '{table_name}' now contains geometry and is ready for spatial queries.")
    except Exception as e:
        print(f"❌ Error during injection: {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

    # 🔎 Test SELECT to check data and geometry (using ST_AsText to display the geometry in WKT)
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"""
            SELECT nom_du_site
            FROM {TABLE_NAME}
            LIMIT 5;
        """)
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

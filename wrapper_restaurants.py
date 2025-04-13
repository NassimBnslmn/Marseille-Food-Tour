import pandas as pd
import psycopg2
from sqlalchemy import create_engine, types, text
from dotenv import load_dotenv
import os
load_dotenv()

# 🗂️ Variables d'environnement
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

TABLE_NAME = "restaurants"
CSV_PATH = "restaurants_final.csv"

# 🧱 Types de colonnes
column_types = {
    "nom": types.Text(),
    "description": types.Text(),
    "adresse": types.Text(),
    "code_postal": types.Integer(),
    "commune": types.Text(),
    "latitude": types.Numeric(9, 6),
    "longitude": types.Numeric(9, 6),
    "periode_ouverte": types.Text(),
    "specialites": types.Text(),
    "review_count": types.Integer(),
    "google_note": types.Numeric(2, 1)
}

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        print(f"📄 Lecture du fichier CSV : {csv_path}")
        df = pd.read_csv(csv_path)

        print(f"📤 Insertion dans la table '{table_name}' avec types explicites...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)

        print(f"✅ Données insérées avec succès dans la table '{table_name}'.")

        # Ajout de la colonne géographique (geog) si elle n'existe pas déjà
        with engine.begin() as connection:
            connection.execute(text("""
                ALTER TABLE restaurants
                ADD COLUMN IF NOT EXISTS geog geography(Point, 4326);
            """))

            connection.execute(text("""
                UPDATE restaurants
                SET geog = ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)::geography
                WHERE longitude IS NOT NULL AND latitude IS NOT NULL;
            """))
            print("🧭 Colonne géographique 'geog' ajoutée et peuplée.")
            
    except Exception as e:
        print(f"❌ Erreur lors de l'injection : {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

    # 🔎 Test rapide de SELECT
    # try:
    #     conn = psycopg2.connect(
    #         dbname=DB_NAME,
    #         user=DB_USER,
    #         password=DB_PASSWORD,
    #         host=DB_HOST,
    #         port=DB_PORT
    #     )
    #     cursor = conn.cursor()
    #     cursor.execute(f"SELECT nom, latitude, longitude FROM {TABLE_NAME} LIMIT 5;")
    #     rows = cursor.fetchall()
    #     for row in rows:
    #         print(row)
    # except Exception as e:
    #     print(f"❌ Erreur lors de la sélection : {e}")
    # finally:
    #     if conn:
    #         cursor.close()
    #         conn.close()
    #         print("✅ Connexion fermée.")

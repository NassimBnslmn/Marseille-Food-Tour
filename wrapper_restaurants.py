import pandas as pd
import psycopg2
from sqlalchemy import create_engine, types

# 🔧 Configuration PostgreSQL
DB_NAME = "food_tour"
DB_USER = "postgres"
DB_PASSWORD = "Karismarseille13="
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = "restaurants"
CSV_PATH = "restaurants_marseille_nettoyes.csv"

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
    "classements": types.Text()
}

def clean_lat_lon(df):
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            # 🔄 Remplacer les virgules par des points et enlever les guillemets
            df[col] = df[col].astype(str).str.replace(",", ".").str.replace('"', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"⚠️ La colonne {col} est manquante dans le fichier CSV.")
    return df

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        
        print(f"📄 Lecture du fichier CSV : {csv_path}")
        df = pd.read_csv(csv_path)

        df["code_postal"] = pd.to_numeric(df["code_postal"], errors='coerce').astype("Int64")
        df = clean_lat_lon(df)  # 🔧 nettoyage de latitude et longitude

        print(f"📤 Insertion dans la table '{table_name}' avec types explicites...")
        df.to_sql(table_name, engine, if_exists='replace', index=False, dtype=column_types)

        print(f"✅ Données insérées avec succès dans la table '{table_name}'.")
    except Exception as e:
        print(f"❌ Erreur lors de l'injection : {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

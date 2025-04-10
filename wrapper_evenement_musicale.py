import pandas as pd
from sqlalchemy import create_engine

# Configuration PostgreSQL
DB_NAME = "food_tour"
DB_USER = "postgres"
DB_PASSWORD = "Karismarseille13="
DB_HOST = "localhost"
DB_PORT = "5432"
TABLE_NAME = "evenements"
CSV_PATH = "evenement_musicale.csv"  # ⚠️ Assure-toi que ce fichier est bien dans le même dossier

def inject_evenements_to_postgres(csv_path, table_name):
    try:
        print(f"📄 Lecture du fichier : {csv_path}")
        df = pd.read_csv(csv_path)

        # Filtrer uniquement les événements situés à Marseille
        df = df[df["Lieu: Ville"].str.lower().str.strip() == "marseille"]

        # Nettoyage et renommage des colonnes utiles
        df_clean = pd.DataFrame({
            "titre": df["Titre"],
            "description": df["Description"],
            "date_debut": pd.to_datetime(df["Horaires ISO"].str.split("->").str[0].str.strip(), errors='coerce'),
            "date_fin": pd.to_datetime(df["Horaires ISO"].str.split("->").str[1].str.strip(), errors='coerce'),
            "nom_lieu": df["Lieu: Nom"],
            "adresse": df["Lieu: Adresse"],
            "code_postal": df["Lieu: Code postal"].astype(str),
            "ville": df["Lieu: Ville"],
            "latitude": pd.to_numeric(df["Lieu: Latitude"], errors='coerce'),
            "longitude": pd.to_numeric(df["Lieu: Longitude"], errors='coerce')
        })

        # Connexion à la base
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        print("🛠️ Connexion à PostgreSQL réussie.")

        # Injection dans la table
        df_clean.to_sql(table_name, engine, if_exists='replace', index=False)
        print(f"✅ Données insérées avec succès dans la table '{table_name}'.")

    except Exception as e:
        print(f"❌ Erreur lors de l'injection : {e}")

# Exécution
if __name__ == "__main__":
    inject_evenements_to_postgres(CSV_PATH, TABLE_NAME)

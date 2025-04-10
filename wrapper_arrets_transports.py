import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 🔧 Configuration PostgreSQL
DB_NAME = "food_tour"
DB_USER = "postgres"
DB_PASSWORD = "Karismarseille13="
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = "arrets_transport"
CSV_PATH = "arrets_transport.csv"  # Remplace par le vrai nom de ton fichier CSV

def nettoyer_donnees_arrets(df):
    # Séparer latitude et longitude à partir de point_geo
    coords = df['point_geo'].str.split(',', expand=True)
    df['latitude'] = pd.to_numeric(coords[0], errors='coerce')
    df['longitude'] = pd.to_numeric(coords[1], errors='coerce')
    
    # Renommer les colonnes pour la base
    df = df.rename(columns={
        'Arrêt': 'nom_arret',
        'Identifiant Interne': 'identifiant_interne',
        'Commune': 'commune',
        'Nom du réseau': 'reseau',
        'Mode de transport': 'mode_transport',
        'Date de MAJ': 'date_maj',
        'Source': 'source'
    })

    # Garder uniquement les colonnes utiles
    colonnes_finales = [
        'nom_arret', 'identifiant_interne', 'commune', 'reseau', 'mode_transport',
        'date_maj', 'source', 'latitude', 'longitude'
    ]
    df = df[colonnes_finales]
    return df

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        print(f"📄 Lecture du fichier CSV : {csv_path}")
        df = pd.read_csv(csv_path, delimiter=';')
        
        df = nettoyer_donnees_arrets(df)
        
        print(f"📤 Insertion dans la table '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"✅ Données insérées avec succès dans la table '{table_name}'.")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)

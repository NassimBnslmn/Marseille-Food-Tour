import pandas as pd
import psycopg2
from sqlalchemy import create_engine

# 🔧 Configuration PostgreSQL
DB_NAME = "food_tour"
DB_USER = "postgres"
DB_PASSWORD = "Karismarseille13="
DB_HOST = "localhost"
DB_PORT = "5432"

TABLE_NAME = "lignes_transport"
CSV_PATH = "lignes_transport.csv"  # À adapter selon ton nom de fichier exact

def nettoyer_donnees_lignes(df):
    # Séparer latitude et longitude à partir de geo_point_2d
    coords = df['geo_point_2d'].str.split(',', expand=True)
    df['latitude'] = pd.to_numeric(coords[0], errors='coerce')
    df['longitude'] = pd.to_numeric(coords[1], errors='coerce')
    
    # Ne garder que les colonnes utiles
    colonnes_utiles = ['ID', 'Short Name', 'Long Name', 'Route Type', 'Color', 'latitude', 'longitude']
    df = df[colonnes_utiles]
    
    # Renommer les colonnes pour la BDD
    df.columns = ['id', 'short_name', 'long_name', 'route_type', 'color', 'latitude', 'longitude']
    
    return df

def inject_csv_to_postgres(csv_path, table_name):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        print(f"📄 Lecture du fichier CSV : {csv_path}")
        df = pd.read_csv(csv_path, delimiter=';')
        
        df = nettoyer_donnees_lignes(df)
        
        print(f"📤 Insertion dans la table '{table_name}'...")
        df.to_sql(table_name, engine, if_exists='replace', index=False)
        
        print(f"✅ Données insérées avec succès dans la table '{table_name}'.")
    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    inject_csv_to_postgres(CSV_PATH, TABLE_NAME)


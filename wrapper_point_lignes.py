import pandas as pd
import psycopg2
import json
from sqlalchemy import create_engine

# 🔧 Configuration PostgreSQL
DB_NAME = "food_tour"
DB_USER = "postgres"
DB_PASSWORD = "root"
DB_HOST = "localhost"
DB_PORT = "5432"

CSV_PATH = "lignes_transport.csv"  # Mets ici le bon chemin vers ton fichier
TABLE_NAME = "points_lignes"

def extraire_points_shape(shape_str):
    try:
        shape_json = json.loads(shape_str.replace('""', '"'))  # Nettoyage du champ JSON
        points = []
        for ligne in shape_json["coordinates"]:
            for coord in ligne:
                lon, lat = coord  # Attention : format [lon, lat]
                points.append((lat, lon))
        return points
    except Exception as e:
        print(f"❌ Erreur lors du parsing du shape : {e}")
        return []

def inject_points_dans_bdd(csv_path, table_name):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df = pd.read_csv(csv_path, sep=';')

        lignes_points = []

        for _, row in df.iterrows():
            ligne_id = row["ID"]
            shape = row.get("Shape", "")
            points = extraire_points_shape(shape)
            for lat, lon in points:
                lignes_points.append({"ligne_id": ligne_id, "latitude": lat, "longitude": lon})

        df_points = pd.DataFrame(lignes_points)

        print(f"📥 Insertion de {len(df_points)} points dans '{table_name}'...")
        df_points.to_sql(table_name, engine, if_exists='replace', index=False)
        print("✅ Insertion réussie !")

    except Exception as e:
        print(f"❌ Erreur lors de l'injection dans la base : {e}")

if __name__ == "__main__":
    inject_points_dans_bdd(CSV_PATH, TABLE_NAME)

    # test select
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST,
            port=DB_PORT
        )
        cursor = conn.cursor()
        cursor.execute(f"SELECT * FROM {TABLE_NAME} LIMIT 5;")
        rows = cursor.fetchall()
        for row in rows:
            print(row)
    except Exception as e:
        print(f"❌ Erreur lors de la sélection : {e}")
    finally:
        cursor.close()
        conn.close()

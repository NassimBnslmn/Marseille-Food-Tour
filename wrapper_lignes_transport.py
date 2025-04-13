import pandas as pd
import psycopg2
import ast
from sqlalchemy import create_engine, types
from dotenv import load_dotenv
import os

load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

CSV_PATH = "lignes_transport_with_stop_ids.csv"

# 🧱 Types pour transport_lines
line_column_types = {
    "id": types.Text(),
    "short_name": types.Text(),
    "long_name": types.Text(),
    "route_type": types.Text(),  # 'Bus', 'Tram', etc.
    "color": types.Text()
}

# 🧱 Types pour line_stops
line_stop_column_types = {
    "line_id": types.Text(),
    "stop_id": types.Text()
}

def inject_lines_and_stops(csv_path):
    try:
        engine = create_engine(f'postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}')
        df = pd.read_csv(csv_path, sep=';')

        df = df.drop(columns=['geo_point_2d'])
        # 📄 Table transport_lines
        df_lines = df[['ID', 'Short Name', 'Long Name', 'Route Type', 'Color']]
        df_lines.columns = ['id', 'short_name', 'long_name', 'route_type', 'color']
        df_lines.to_sql('lignes_transport', engine, if_exists='replace', index=False, dtype=line_column_types)
        print("✅ Table 'lignes_transport' créée.")

        # 🔄 Parser les stop_ids et construire line_stops
        line_stop_pairs = []

        for _, row in df.iterrows():
            line_id = row['ID']
            try:
                stop_ids = ast.literal_eval(row['stop_ids'])  # transforme la string en liste Python
                for stop_id in stop_ids:
                    line_stop_pairs.append({'line_id': line_id, 'stop_id': stop_id})
            except Exception as e:
                print(f"⚠️ Erreur parsing stop_ids pour la ligne {line_id} : {e}")

        df_line_stops = pd.DataFrame(line_stop_pairs)
        df_line_stops.to_sql('arrets_lignes_transport', engine, if_exists='replace', index=False, dtype=line_stop_column_types)
        print("✅ Table 'arrets_lignes_transport' créée.")

    except Exception as e:
        print(f"❌ Erreur : {e}")

if __name__ == "__main__":
    inject_lines_and_stops(CSV_PATH)

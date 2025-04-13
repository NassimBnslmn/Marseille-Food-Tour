import os
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

# 🔐 Connexion
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# 📂 Chemin des fichiers SQL
sql_views_path = "sql/views"

# 🧱 Connexion SQLAlchemy
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# 📋 Liste ordonnée des vues à supprimer (les dépendances vont de haut en bas)
MATERIALIZED_VIEWS = [
    "mini_itineraire_event_restaurant_arret",
    "restaurant_nearest_event",
    "restaurant_nearest_stops",
        "evenements_arrets",
    "baignades_nearest_stops"
]

VIEWS = [
    "nb_restaurants_par_arret",
    "nb_evenements_par_arret",
    "baignades_meteo",
    "avg_temp_per_baignade"
]

def drop_all_views():
    print("🧹 Dropping MATERIALIZED VIEWS...")
    with engine.begin() as conn:
        for view in MATERIALIZED_VIEWS:
            try:
                conn.execute(text(f"DROP MATERIALIZED VIEW IF EXISTS {view} CASCADE;"))
                print(f"✅ Dropped materialized view: {view}")
            except Exception as e:
                print(f"❌ Failed to drop materialized view {view}: {e}")

    print("🧼 Dropping VIEWS...")
    with engine.begin() as conn:
        for view in VIEWS:
            try:
                conn.execute(text(f"DROP VIEW IF EXISTS {view} CASCADE;"))
                print(f"✅ Dropped view: {view}")
            except Exception as e:
                print(f"❌ Failed to drop view {view}: {e}")

def generate_views():
    print("🚀 Generating views from SQL files...")
    for root, _, files in os.walk(sql_views_path):
        for file in sorted(files):
            if file.endswith(".sql"):
                file_path = os.path.join(root, file)
                with open(file_path, 'r', encoding='utf-8') as sql_file:
                    sql_query = sql_file.read()
                    try:
                        with engine.begin() as connection:
                            connection.execute(text(sql_query))
                            print(f"✅ Executed: {file}")
                    except Exception as e:
                        print(f"❌ Error in {file}: {e}")

if __name__ == "__main__":
    drop_all_views()
    generate_views()

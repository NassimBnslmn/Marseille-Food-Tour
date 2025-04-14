from dotenv import load_dotenv
import os
import pandas as pd
from sqlalchemy import create_engine
import folium
from shapely import wkt
import webbrowser

# Load environment variables from a .env file
load_dotenv()

# Get database connection parameters from environment variables
DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")

# Build the PostgreSQL connection string
connection_string = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
engine = create_engine(connection_string)

# SQL query converting geometry to WKT for restaurant and event.
sql = """
SELECT restaurant_name, 
    note, 
    review_count, 
    ST_AsText(geog_restaurant) as geog_restaurant,
    ST_AsText(geog_evenement) as geog_evenement,
    distance,
    evenement,
    date_debut, date_fin
FROM restaurant_nearest_event
WHERE note >= 4.0
  AND review_count >= 3 
  AND distance <= 200
  AND date_debut <= NOW()
  
ORDER BY evenement
"""

# Use pandas to execute the query.
df = pd.read_sql_query(sql, engine)

# Convert the WKT strings into Shapely geometry objects using the correct column names.
df["restaurant_geom"] = df["geog_restaurant"].apply(wkt.loads)
df["event_geom"] = df["geog_evenement"].apply(wkt.loads)

# Optionally, print the dataframe to check that it loaded correctly.
print(df.head())

# Create a Folium map centered on Marseille.
map_center = [43.296482, 5.36978]  # [latitude, longitude]
m = folium.Map(location=map_center, zoom_start=12)

# Loop through each record to add markers to the map.
for idx, row in df.iterrows():
    # Extract coordinates from the Shapely geometry objects.
    # Note: Shapely geometry has .x for longitude and .y for latitude.
    r_lon, r_lat = row["restaurant_geom"].x, row["restaurant_geom"].y
    e_lon, e_lat = row["event_geom"].x, row["event_geom"].y

    # Create a popup for the restaurant.
    popup_restaurant = (
        f"<b>{row['restaurant_name']}</b><br>"
        f"Rating: {row['note']} ⭐<br>"
        f"Reviews: {row['review_count']}<br>"
        f"Distance: {row['distance']:.2f} m"
    )
    folium.Marker(
        location=[r_lat, r_lon],
        popup=popup_restaurant,
        icon=folium.Icon(color="blue", icon="cutlery", prefix="fa")
    ).add_to(m)

    # Create a popup for the event.
    popup_event = f"<b>Event:</b> {row['evenement']}"
    folium.Marker(
        location=[e_lat, e_lon],
        popup=popup_event,
        icon=folium.Icon(color="red", icon="music", prefix="fa")
    ).add_to(m)

# Save the map to an HTML file and automatically open it in the default browser.
map_file = "map_with_markers.html"
m.save(map_file)
print(f"Map saved as {map_file}")
webbrowser.open(f"file://{os.path.realpath(map_file)}")

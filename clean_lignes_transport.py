import pandas as pd
import json

# Load the CSVs
lignes_df = pd.read_csv("lignes_transport.csv", sep=";")
arrets_df = pd.read_csv("cleaned_arrets.csv")

# Function to normalize latitude and longitude to 4 decimal places
def normalize_coordinates(lat, lon):
    return round(lat, 4), round(lon, 4)

# Function to parse the Shape column (in JSON format) and return a list of normalized coordinates
def parse_shape(shape_str):
    try:
        shape_json = json.loads(shape_str.replace('""', '"'))  # Clean up the shape data
        coordinates = []
        for line in shape_json["coordinates"]:
            for coord in line:
                lat, lon = normalize_coordinates(coord[1], coord[0])  # Normalize to 4 decimal places
                coordinates.append((lat, lon))
        return coordinates
    except Exception as e:
        print(f"❌ Erreur lors du parsing du Shape : {e}")
        return []

# Function to parse the Coordinates column in cleaned_arrets.csv
def parse_arret_coordinates(coords_str):
    coords = coords_str.strip().replace('"', '').split(", ")
    lat, lon = float(coords[0]), float(coords[1])
    return normalize_coordinates(lat, lon)

# Add latitude and longitude columns to arrets_df with normalized coordinates
arrets_df[['latitude', 'longitude']] = arrets_df['Coordinates'].apply(lambda x: pd.Series(parse_arret_coordinates(x)))

# Create a dictionary for fast lookup of stop IDs based on normalized coordinates
arret_dict = {}
for _, row in arrets_df.iterrows():
    lat_lon = (round(row['latitude'], 4), round(row['longitude'], 4))
    arret_dict[lat_lon] = row['ID']

# Function to find the closest arret ID by matching normalized coordinates
def find_closest_arret(lat, lon, arret_dict):
    normalized_coords = (round(lat, 4), round(lon, 4))
    return arret_dict.get(normalized_coords)

# Iterate over the rows in lignes_df and process the Shape coordinates
def replace_shape_with_arret_ids():
    lignes_df['stop_ids'] = lignes_df['Shape'].apply(lambda shape: [find_closest_arret(lat, lon, arret_dict) for lat, lon in parse_shape(shape)])

# Run the replacement
replace_shape_with_arret_ids()

# Remove None values from the 'stop_ids' column
lignes_df['stop_ids'] = lignes_df['stop_ids'].apply(lambda x: [id for id in x if id is not None])

# drop Shape column if it exists
if 'Shape' in lignes_df.columns:
    lignes_df.drop(columns=['Shape'], inplace=True)

# Drop Route URL column if it exists
if 'Route URL' in lignes_df.columns:
    lignes_df.drop(columns=['Route URL'], inplace=True)

lignes_df = lignes_df[lignes_df['stop_ids'].str.len() > 0]

# Save the result to a new CSV with stop IDs instead of coordinates
lignes_df.to_csv("lignes_transport_with_stop_ids.csv", index=False, sep=";")

print("✅ Successfully updated 'lignes_transport.csv' with stop IDs, removing Nones.")

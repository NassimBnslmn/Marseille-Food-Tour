import pandas as pd
import os
import requests
from dotenv import load_dotenv
load_dotenv()
# Get the Yelp API key from environment variable
YELP_API_KEY = os.getenv("YELP_API_KEY")
HEADERS = {"Authorization": f"Bearer {YELP_API_KEY}"}
print(f"Yelp API Key: {YELP_API_KEY}")

def get_yelp_rating(name, location):
    url = "https://api.yelp.com/v3/businesses/search"
    params = {
        "term": name,
        "location": location,
        "limit": 1
    }

    response = requests.get(url, headers=HEADERS, params=params)
    if response.status_code != 200:
        print(f"Error fetching data for {name}: {response.status_code}")
        return None, None

    data = response.json()
    if data.get("businesses"):
        business = data["businesses"][0]
        return business.get("rating"), business.get("review_count")
    else:
        return None, None

def enrich_csv_with_ratings(csv_input_path, csv_output_path):
    # Load the CSV with encoding fix
    df = pd.read_csv(csv_input_path, sep=",")

    # Normalize column names
    df.columns = df.columns.str.strip().str.lower()

    # Add new columns for rating and review count
    df["google_note"] = None
    df["review_count"] = None

    for index, row in df.iterrows():
        name = row["nom"]
        location = f'{row["adresse"]}, {row["commune"]}'

        rating, review_count = get_yelp_rating(name, location)
        print(f"{index+1}/{len(df)} - {name}: rating={rating}, reviews={review_count}")

        df.at[index, "google_note"] = rating
        df.at[index, "review_count"] = review_count


    df.to_csv(csv_output_path, index=False, sep=",")
    print(f"Enriched data saved to {csv_output_path}")

enrich_csv_with_ratings("restaurants_marseille_nettoyes.csv", "restaurants_with_ratings.csv")

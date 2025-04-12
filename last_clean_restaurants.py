import pandas as pd

def clean_lat_lon(df):
    for col in ["latitude", "longitude"]:
        if col in df.columns:
            # 🔄 Remplacer les virgules par des points et enlever les guillemets
            df[col] = df[col].astype(str).str.replace(",", ".").str.replace('"', '').str.strip()
            df[col] = pd.to_numeric(df[col], errors='coerce')
        else:
            print(f"⚠️ La colonne {col} est manquante dans le fichier CSV.")
    return df

def clean_and_modify_csv(csv_input_path, csv_output_path):
    # Load the CSV
    df = pd.read_csv(csv_input_path, sep=",")

    clean_lat_lon(df)  # Clean latitude and longitude

    # Remove the 'classements' column
    if 'classements' in df.columns:
        df.drop(columns=['classements'], inplace=True)
    if 'rating' in df.columns:
        df.drop(columns=['rating'], inplace=True)

    # Update review_count and google_note to None where they are 0
    df.loc[(df['review_count'] == 0) & (df['google_note'] == 0), ['review_count', 'google_note']] = None, None

    # 
    if 'review_count' in df.columns:
        df['review_count'] = df['review_count'].astype(float).round(0).astype('Int64')  # Use Int64 to keep NaNs

    # Save the modified dataframe back to a CSV
    df.to_csv(csv_output_path, index=False, sep=",")

# Example usage
clean_and_modify_csv("restaurants_with_ratings.csv", "restaurants_final.csv")

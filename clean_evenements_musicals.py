import pandas as pd

# Load the CSVs
df = pd.read_csv("evenement_musicale.csv", sep=",")

# find frequency ratio of Marseille in Lieu: Ville
# drop rows where Lieu: Ville is not Marseille
df = df[df["Lieu: Ville"] == "Marseille"]

# Split the Horaires ISO column
df["date_debut"] = df["Horaires ISO"].str.split("->").str[0].str.strip()
df["date_fin"] = df["Horaires ISO"].str.split("->").str[1].str.strip()

# Optionally convert to datetime
df["date_debut"] = pd.to_datetime(df["date_debut"], errors="coerce")
df["date_fin"] = pd.to_datetime(df["date_fin"], errors="coerce")

# drop column test
df.drop(columns=["Horaires ISO", "Horaires détaillés","Dernière date"], inplace=True)

df = df.rename(columns={
    "Titre": "titre",
    "Description": "description",
    "Lieu: Nom": "nom_lieu",
    "Lieu: Adresse": "adresse",
    "Lieu: Code postal": "code_postal",
    "Lieu: Ville": "ville",
    "Lieu: Latitude": "latitude",
    "Lieu: Longitude": "longitude"
})
# remove decimal from Lieu: Code postal
df["code_postal"] = df["code_postal"].astype(int)

df.to_csv("evenement_musical_cleaned.csv", index=False, sep=",")
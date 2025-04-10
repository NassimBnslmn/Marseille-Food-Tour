import pandas as pd
import json

# Charger le fichier CSV
df = pd.read_csv("restaurants.csv", sep=';')

# Colonnes contenant des objets JSON
json_cols = ['nom', 'presentation', 'localisation', 'ouverture', 'informationsRestauration']

# Fonction pour parser chaque champ JSON
def parse_json_column(value):
    try:
        return json.loads(value)
    except:
        return {}

# Appliquer à toutes les colonnes JSON
for col in json_cols:
    df[col] = df[col].apply(parse_json_column)

# Extraire les champs utiles
df_extract = pd.DataFrame({
    'nom': df['nom'].apply(lambda x: x.get('libelleFr', '')),
    'description': df['presentation'].apply(lambda x: x.get('descriptifCourt', {}).get('libelleFr', '')),
    'adresse': df['localisation'].apply(lambda x: x.get('adresse', {}).get('adresse1', '')),
    'code_postal': df['localisation'].apply(lambda x: x.get('adresse', {}).get('codePostal', '')),
    'commune': df['localisation'].apply(lambda x: x.get('adresse', {}).get('commune', {}).get('nom', '')),
    'latitude': df['lat'],
    'longitude': df['lon'],
    'periode_ouverte': df['ouverture'].apply(lambda x: x.get('periodeEnClair', {}).get('libelleFr', '')),
    'specialites': df['informationsRestauration'].apply(
        lambda x: ', '.join([s.get('libelleFr', '') for s in x.get('specialites', [])]) if 'specialites' in x else ''
    ),
    'classements': df['informationsRestauration'].apply(
        lambda x: ', '.join([c.get('libelleFr', '') for c in x.get('classementsGuides', [])]) if 'classementsGuides' in x else ''
    )
})

# Filtrer uniquement ceux situés à Marseille
df_marseille = df_extract[df_extract['commune'].str.lower().str.contains("marseille", na=False)]

# Sauvegarder dans un nouveau fichier CSV
df_marseille.to_csv("restaurants_marseille_nettoyes.csv", index=False, encoding='utf-8-sig')

print("✅ Fichier filtré et sauvegardé sous : restaurants_marseille_nettoyes.csv")

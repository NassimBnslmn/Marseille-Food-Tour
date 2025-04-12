import pandas as pd

# Load the CSVs
lignes_df = pd.read_csv("lignes_transport_with_stop_ids.csv", sep=";")
arrets_df = pd.read_csv("cleaned_arrets.csv")

# Convert 'stop_ids' column from string to list
lignes_df['stop_ids'] = lignes_df['stop_ids'].apply(lambda x: eval(x) if isinstance(x, str) else [])

# Flatten all stop_ids from 'lignes_df' into a set for quick lookup
all_stop_ids_in_lignes = set([item for sublist in lignes_df['stop_ids'] for item in sublist])

# Find arrets whose IDs are NOT in the 'stop_ids' list from lignes_df
arrets_not_in_lignes = arrets_df[~arrets_df['ID'].isin(all_stop_ids_in_lignes)]

# Output the names of arrets not in lignes
if not arrets_not_in_lignes.empty:
    print("Arrets whose IDs are NOT in the lignes_transport file:")
    for name in arrets_not_in_lignes['Name']:
        print(name)
else:
    print("All arret IDs are present in the lignes_transport file.")

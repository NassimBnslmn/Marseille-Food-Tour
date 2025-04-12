import pandas as pd

# Load the CSV file
file_path = 'arrets.csv'
df = pd.read_csv(file_path, sep=";")

# List of columns to delete
columns_to_delete = ['Code', 'Zone ID', 'URL', 'Location Type', 'Parent Station ID', 'Timezone']

# Drop the specified columns
df = df.drop(columns=columns_to_delete, errors='ignore')

# Save the cleaned CSV back to a file
output_file_path = 'cleaned_arrets.csv'
df.to_csv(output_file_path, index=False, sep=",")

print(f"Cleaned CSV saved to {output_file_path}")
import pandas as pd

# Ruta al archivo original
file_path = 'data/processed/Users_Train.csv'

# Cargar datos
df = pd.read_csv(file_path)

# Tomar muestra aleatoria reproducible de 1000 filas
df_sample = df.sample(n=1000, random_state=42)

# Reducir columnas
df_sample_reduced = df_sample[['Id', 'CreationDate', 'Views', 'UpVotes']]

# Guardar CSV reducido
output_path = 'struct-sst/data/stackexchange_users.csv'
df_sample_reduced.to_csv(output_path, index=False)

print(f"Archivo guardado en: {output_path}")

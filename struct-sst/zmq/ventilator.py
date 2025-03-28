import pandas as pd

file_path = 'data/processed/Users_Train.csv'

df = pd.read_csv(file_path)

df_sample = df.sample(n=1000, random_state=42)


# Mostrar las primeras filas de las columnas deseadas
df_sample_reduced = df_sample[['Id', 'CreationDate', 'Views', 'UpVotes']].head()

# Enviar fila como string (por ejemplo a través de un socket ZeroMQ)
for index, row in df_sample_reduced.iterrows():
    row_string = row.to_string()  # Convirtiendo cada fila en una cadena
    # row_string = row_string.replace('\t', '')  # Eliminando saltos de línea
    # row_string = row_string.replace('\n', '|')  # Eliminando saltos de línea
    print(row_string)
    # Aquí enviarías row_string al worker, usando un socket ZMQ, por ejemplo:
    # socket.send_string(row_string)
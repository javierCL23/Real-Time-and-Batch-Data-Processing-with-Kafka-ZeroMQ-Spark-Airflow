import pandas as pd
import zmq
import json

# Cargar datos
file_path = '../../data/processed/Users_Train.csv'
df = pd.read_csv(file_path)

# Tomar muestra
df_sample = df.sample(n=1000, random_state=42)

# Reducir columnas
df_sample_reduced = df_sample[['Id', 'CreationDate', 'Views', 'UpVotes']]

# Configurar ZeroMQ
context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")  # Este bind es para que los workers se conecten

# Enviar cada fila como JSON
for _, row in df_sample_reduced.iterrows():
    row_dict = {
        "Id": int(row["Id"]),
        "CreationDate": row["CreationDate"],
        "Views": int(row["Views"]),
        "UpVotes": int(row["UpVotes"])
    }
    row_json = json.dumps(row_dict)  # Convertir a JSON
    sender.send_string(row_json)  # Enviar

import pandas as pd
import zmq
import json
import logging 
import os

"""if os.path.exists("struct-sst/zmq/ZMQ_logs.log"):
    os.remove("struct-sst/zmq/ZMQ_logs.log")  # Borra el archivo al inicio
"""

# Configuración de logging
processName = "VENTILATOR"
logger = logging.getLogger(processName)

logging.basicConfig(
    filename='struct-sst/zmq/ventilador.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# Cargar datos
file_path = 'data/processed/Users_Train.csv'
df = pd.read_csv(file_path)

# Tomar muestra
df_sample = df.sample(n=1000, random_state=42)

# Reducir columnas
df_sample_reduced = df_sample[['Id', 'CreationDate', 'Views', 'UpVotes']]

# Configurar ZeroMQ
context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")  # Este bind es para que los workers se conecten

logger.info(f"Ventilator is ready to begin.")
# Esperar que los workers estén listos
print("Press Enter when the workers and sink are ready: ")
_ = input()  # Espera a que se presione Enter

print("Sending tasks to workers...")
logger.info("Workers are ready, sending tasks to workers...")

i = 0
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
    i+=1
    if i%100 == 0:
        logger.info(f"{i}/1000 items where sent")

# Finalización
logger.info("Ventilator finished sending tasks.")
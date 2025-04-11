import pandas as pd
import zmq
import json
import logging 


# Configuración de logging
processName = "VENTILATOR"
logger = logging.getLogger(processName)

logging.basicConfig(
    filename='struct-sst/zmq/ventilador_zmq.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
    )

# Cargar datos
file_path = 'data/processed/UsersSubsample.csv'
df = pd.read_csv(file_path)

# Configurar ZeroMQ
context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.bind("tcp://*:5557")  # Este bind es para que los workers se conecten

logger.info(f"Ventilator is ready to begin.")
# Esperar que los workers estén listos
print("Press Enter when the workers and sink are ready: ")
_ = input()  # Espera a que se presione Enter (No sería necesaria si se ejecuta todo con el ejecutor_ZMQ.sh)

print("Sending tasks to workers...")
logger.info("Workers are ready, sending tasks to workers...")

i = 0
# Enviar cada fila como JSON
for _, row in df.iterrows():
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
        #Cada 100 mensajes se informa de que se han enviado n mensajes
        logger.info(f"{i}/1000 items where sent")

# Finalización
logger.info("Ventilator finished sending tasks.")


import logging
import zmq
import json
import toml

tml = toml.load("pyproject.toml")
context = zmq.Context()

# Recibir mensajes
reciever = context.socket(zmq.PULL)
reciever.bind("tcp://*:5558")

# Control de workers
controller = context.socket(zmq.PUB)
controller.bind("tcp://*:5559")

# Configuración de logs
processName = "SINK"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename='struct-sst/zmq/sink_zmq.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
    )

#----------------------MAIN--------------------------
n=1000
resultados = {}
n_resultados = 0
logger.info(f"Sink is ready to recieve.")
for i in range(n):
    fecha = reciever.recv_string()
    if fecha in resultados:
        #Si ya existe la fecha, le suma 1
        resultados[fecha]+=1
    else:
        #Si no existe, la crea con un conteo de 1
        resultados[fecha] = 1
    n_resultados += 1
    if n_resultados%100 == 0:
        #Cada 100 mensajes se informa de que se han enviado n mensajes
        logger.info(f"Progress --> {n_resultados}/{n}")


#Finalización

#Mandar sigkill a los workers
controller.send(b"KILL")

#Cerrar comunicaciones y contexto
reciever.close()
controller.close()
context.term()

#Escribir resultados
with open(tml["tools"]["project_paths"]["results_zmq_path"], 'w') as file:
    json.dump(dict(sorted(resultados.items())),file,indent=2)
    
logger.info('Sink Finished')

import logging
import sys
import zmq
import json

context = zmq.Context()

# Recibir mensajes
reciever = context.socket(zmq.PULL)
reciever.bind("tcp://*:5558")

# Control de workers
controller = context.socket(zmq.PUB)
controller.bind("tcp://*:5559")


processName = "SINK"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename='struct-sst/zmq/sink_zmq.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)
#----------------------MAIN--------------------------
n=1000
resultados = {}
n_resultados = 0
logger.info(f"Sink is ready to recieve.")
for i in range(n):
    fecha = reciever.recv_string()
    if fecha in resultados:
        resultados[fecha]+=1
    else:
        resultados[fecha] = 1
    n_resultados += 1
    if n_resultados%100 == 0:
        logger.info(f"Progress --> {n_resultados}/{n}")


#Finalización
controller.send(b"KILL")

reciever.close()
controller.close()
context.term()

with open('struct-sst/zmq/final-results_zmq.txt', 'w') as file:
    json.dump(dict(sorted(resultados.items())),file,indent=2)
    
logger.info('Sink Finished')

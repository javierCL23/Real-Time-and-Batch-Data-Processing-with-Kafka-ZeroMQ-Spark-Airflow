import logging
import sys
import zmq
import json

context = zmq.Context()

# Recibir mensajes
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://*:5558")

# Control de workers
controller = context.socket(zmq.PUB)
controller.bind("tcp://*:5559")


processName = "SINK"
logger = logging.getLogger(processName)
logging.basicConfig(
        filename='struct-sst/zmq/sink.log',
        filemode='w',  # Para sobreescribir si ya existe el archivo
        format="|{name}|[{asctime}]:{levelname} - {message}",
        style="{", 
        datefmt="%Y-%m-%d %H:%M")

logger.setLevel(logging.INFO)

#----------------------MAIN--------------------------
n=1000
resultados = {}
n_resultados = 0

for i in range(n):
    fecha = receiver.recv_string()
    if fecha in resultados:
        resultados[fecha]+=1
    else:
        resultados[fecha] = 1
    n_resultados += 1
    if n_resultados%100 == 0:
        logger.info(f"Progress --> {n_resultados}/{n}")


#Finalización
controller.send(b"KILL")

receiver.close()
controller.close()
context.term()
print(resultados)
with open('struct-sst/zmq/resultados.txt', 'w') as file:
    file.write(json.dumps(resultados))
    
logger.info('Finished')
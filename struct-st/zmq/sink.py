import logging
import sys
import zmq


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
        filename='pruebas.log',
        format="|{name}|[{asctime}]:{levelname} - {message}",
        style="{", 
        datefmt="%Y-%m-%d %H:%M")

logger.setLevel(logging.INFO)

#----------------------MAIN--------------------------
n=1000
resultados = {}
n_resultados = 0

for i in range(n):
    fecha = receiver.recv()
    if fecha in resultados:
        resultados[fecha]+=1
    else:
        resultados[fecha] = 1
    n_resultados += 1
    if n_resultados%100 == 0:
        logger.info("Progress --> {n_resultados}/{n}")


#Finalización
controller.send(b"KILL")

receiver.close()
controller.close()
context.term()

logger.info('Finished')
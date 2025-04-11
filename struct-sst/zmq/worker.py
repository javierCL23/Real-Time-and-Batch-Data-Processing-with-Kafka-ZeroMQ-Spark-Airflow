import zmq
import logging
import sys
import json

#Configuración del log
if len(sys.argv) < 2:
    raise ValueError("An argument is nedeed to identify the worker.")

id = sys.argv[1]

processName = f"WORKER{id}"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename=f'struct-sst/zmq/worker{id}_zmq.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
    )

# Configuración ZMQ
context = zmq.Context()

#Para recibir mensajes desde el vent
receiver = context.socket(zmq.PULL)
receiver.connect("tcp://localhost:5557")

#Para enviar mensajes al sink
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5558")

#Para comprobar si ya no quedan tareas
controller = context.socket(zmq.SUB)
controller.connect("tcp://localhost:5559")
controller.setsockopt(zmq.SUBSCRIBE, b"")

#Para poder recibir mensajes en general registrando tanto mensajes del vent, como mensajes del sink para parar
poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(controller, zmq.POLLIN)

# MAIN
count = 0
logger.info(f"Starting processing in worker{id}.")
while True:
    #Recibimos mensaje
    socks = dict(poller.poll())

    #Caso de mensaje proveniente del vent
    if socks.get(receiver) == zmq.POLLIN:
        message = json.loads(receiver.recv_string())
        #Enviamos mensaje al sink con el año que está formateado como YYYY-MM-DD (solo nos quedamos con el año)
        sender.send_string(f"{message['CreationDate'][:4]}")
        count += 1

    if count%100 == 0:
        #Cada 100 mensajes se informa de que se han enviado n mensajes
        logger.info(f"{count} items were processed.")

    #Caso de mensaje proveniente del sink
    if socks.get(controller) == zmq.POLLIN:
        logger.info(f"Ending processing in worker{id}. {count} items processed")
        break
    

import zmq
import logging
import sys 
from time import sleep
import json

if len(sys.argv) < 2:
    raise ValueError("Se requiere un argumento para identificar el worker.")

num = sys.argv[1]

processName = f"WORKER{num}"
logger = logging.getLogger(processName)
logging.basicConfig(
        filename='pruebas.log',
        format="|{name}|[{asctime}]:{levelname} - {message}",
        style="{", 
        datefmt="%Y-%m-%d %H:%M")
logger.setLevel(logging.INFO)


context = zmq.Context()

receiver = context.socket(zmq.PULL)
receiver.connect("tcp://localhost:5557")

sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5558")

controller = context.socket(zmq.SUB)
controller.connect("tcp://localhost:5559")
controller.setsockopt(zmq.SUBSCRIBE, b"")

poller = zmq.Poller()
poller.register(receiver, zmq.POLLIN)
poller.register(controller, zmq.POLLIN)

count = 0
logger.info(f"Iniciando procesamiento en worker{num}.")
while True:

    if count%100 == 0:
        logger.info(f"{count} items procesados.")

    socks = dict(poller.poll())

    if socks.get(receiver) == zmq.POLLIN:
        message = json.loads(receiver.recv_string())

        sender.send_string(f"{message['CreationDate'].split('-')[0]}")
        count += 1


    if socks.get(controller) == zmq.POLLIN:
        logger.info(f"Finalizando procesamiento en worker{num}.\n")
        break
    

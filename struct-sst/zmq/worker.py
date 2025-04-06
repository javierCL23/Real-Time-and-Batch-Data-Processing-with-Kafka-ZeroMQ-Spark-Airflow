import zmq
import logging
import sys 
from time import sleep
import json

if len(sys.argv) < 2:
    raise ValueError("An argument is nedeed to identify the worker.")

id = sys.argv[1]

processName = f"WORKER{id}"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename=f'struct-sst/zmq/worker{id}_zmq.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

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
logger.info(f"Starting processing in worker{id}.")
while True:
    socks = dict(poller.poll())

    if socks.get(receiver) == zmq.POLLIN:
        message = json.loads(receiver.recv_string())

        sender.send_string(f"{message['CreationDate'][:4]}")
        count += 1

    if count%100 == 0:
        logger.info(f"{count} items were processed.")


    if socks.get(controller) == zmq.POLLIN:
        logger.info(f"Ending processing in worker{id}. {count} items processed")
        break
    

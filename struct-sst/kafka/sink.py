import logging
import zmq
import sys
import json

context = zmq.Context()

# Puerto desde el que recibirá mensajes de los consumers
reciever = context.socket(zmq.PULL)
reciever.bind("tcp://*:5557")

processName = "SINK"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename='struct-sst/zmq/sink_kafka.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# Proceso de ZMQ
final_results = {}
logging.info("Sink ready to recieve messages.")
for i in range(3):
    results = reciever.recv_json()
    for year,count in results.items():
        if year in final_results:
            final_results[year]+=count
        else:
            final_results[year]=count
    logging.info(f"{i}/3 Messages Recieved.")


#Finalización
reciever.close()
context.term()

with open('struct-sst/zmq/final-results_kafka.txt', 'w') as file:
    file.write(json.dumps(final_results))

logger.info('Sink Finished')
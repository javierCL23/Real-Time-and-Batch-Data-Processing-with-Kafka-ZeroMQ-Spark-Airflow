import logging
import zmq
import json

import time
context = zmq.Context()

# Puerto desde el que recibirá mensajes de los consumers
receiver = context.socket(zmq.PULL)
receiver.bind("tcp://*:5557")

processName = "SINK"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename='struct-sst/kafka/sink_kafka.log',
    #filename='sink_kafka.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


# Proceso de ZMQ
final_results = {}
logger.info("Sink ready to receive results.")
for i in range(3):
    results = receiver.recv_json()
    for year,count in results.items():
        if year in final_results:
            final_results[year]+=count
        else:
            final_results[year]=count
    logger.info(f"{i+1}/3 Messages Received.")

#Finalización
receiver.close()
context.term()
logger.info('Sink has closed ZMQ comunications')


with open('struct-sst/kafka/final-results_kafka.txt', 'w') as file:
#with open('final-results_kafka.txt', 'w') as file:
    json.dump(dict(sorted(final_results.items())), file, indent=2)

logger.info('Sink Finished')
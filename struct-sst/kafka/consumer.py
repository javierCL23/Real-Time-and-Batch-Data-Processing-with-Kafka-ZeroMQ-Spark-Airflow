from confluent_kafka import Consumer
import json
import logging
import sys 
import zmq

# -------------------------------- ZMQ ------------------------------------------
if len(sys.argv) < 2:
    raise ValueError("An argument is nedeed to identify the consumer.")

id = sys.argv[1]

processName = f"CONSUMER{id}"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename=f'struct-sst/kafka/consumer{id}.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5557")

# -------------------------------- Kafka ------------------------------------------
config = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092',
    'group.id':          'consumers-kafka',
    'enable.auto.commit': 'false',
    'auto.offset.reset': 'latest'
}

consumer = Consumer(config)

topic_msg = "items-GR-1"
topic_stop = "caballero, caballero, detengase"
consumer.subscribe([topic_msg, topic_stop])


results = dict()
count = 0
while True:
        message = consumer.poll(1.0)

        if message.error():
            print(f"ERROR: {message.error()}")
        else:
            if message.topic() == topic_stop:
                sender.send_json(json.dumps(results))
                logger.info(f"Ending processing in worker{id}. {count} items processed.")
                break
            else:
                result = message.value()['CreationDate'][:4]
                if result in results:
                    results[result] += 1
                else:
                    results[result] = 1
                
            if count%100 == 0:
                logger.info(f"{count} items were processed.")
            count+=1

consumer.unsubscribe()
consumer.close()



from kafka import KafkaConsumer
import zmq
import json
import logging
import time

logging.basicConfig(filename='consumer0.log', level=logging.INFO)

TOPIC = 'items-GR-1'
BROKER = '10.110.100.76:9092'
PORT_ZMQ = 5557

consumer = KafkaConsumer(
    TOPIC,
    bootstrap_servers=BROKER,
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='earliest',
    group_id='grupo1',
    consumer_timeout_ms=1000
)

ctx = zmq.Context()
socket = ctx.socket(zmq.PUSH)
socket.connect(f"tcp://localhost:{PORT_ZMQ}")

results = {}
data_list = []

start = time.time()
timeout_count = 0
max_timeout = 10
i = 0

for message in consumer:
    if message is None:
        timeout_count += 1
        if timeout_count >= max_timeout:
            break
        continue
    timeout_count = 0

    data = message.value
    year = data['CreationDate'][:4]
    results[year] = results.get(year, 0) + 1
    data_list.append(data)

    if i % 100 == 0:
        logging.info(f"{i} mensajes procesados")
    i += 1

socket.send_json({'consumer': 0, 'results': results})

with open('consumer0_output.json', 'w') as f:
    json.dump(data_list, f)

logging.info(f"Tiempo de escritura json: {time.time() - start:.2f} segundos")

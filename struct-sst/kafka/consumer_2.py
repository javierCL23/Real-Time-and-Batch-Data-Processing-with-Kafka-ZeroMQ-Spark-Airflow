from kafka import KafkaConsumer
import zmq
import pyarrow as pa
import pyarrow.parquet as pq
import logging
import time
import json

logging.basicConfig(filename='consumer2.log', level=logging.INFO)

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

socket.send_json({'consumer': 2, 'results': results})

table = pa.Table.from_pylist(data_list)
pq.write_table(table, 'consumer2_output.parquet')

logging.info(f"Tiempo de escritura Parquet: {time.time() - start:.2f} segundos")

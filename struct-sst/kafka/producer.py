import csv
import time
from kafka import KafkaProducer, KafkaAdminClient
from kafka.admin import NewTopic
from kafka.errors import TopicAlreadyExistsError
import json
import logging

logging.basicConfig(filename='producer.log', level=logging.INFO)

TOPIC = 'items-GR-1'
BROKER = '10.110.100.76:9092'
CSV_FILE = '../data/stackexchange_users.csv'

def create_topic():
    admin = KafkaAdminClient(bootstrap_servers=BROKER)
    try:
        admin.delete_topics([TOPIC])
        time.sleep(1)  # espera breve tras borrar
    except Exception:
        pass
    try:
        topic = NewTopic(name=TOPIC, num_partitions=3, replication_factor=1) #3 particiones porque hay 3 consumers, 1 réplica porque solo hay un broker
        admin.create_topics([topic])
        print(f"Topic {TOPIC} creado")
    except TopicAlreadyExistsError:
        print(f"Topic {TOPIC} ya existe")

def run_producer():
    create_topic()
    producer = KafkaProducer(
        bootstrap_servers=BROKER,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    with open(CSV_FILE, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        for i, row in enumerate(reader):
            producer.send(TOPIC, row)
            if i % 100 == 0:
                logging.info(f"{i} elementos enviados")
        producer.flush()
        logging.info("Todos los elementos han sido enviados")

if __name__ == '__main__':
    run_producer()

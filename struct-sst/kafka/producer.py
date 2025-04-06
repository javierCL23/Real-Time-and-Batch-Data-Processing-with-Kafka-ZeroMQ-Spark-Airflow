import csv
import time
import json
import logging
import os
from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configuración
BROKER = '10.110.100.76:9092' 
DATA_TOPIC = 'items-GR-1'
CONTROL_TOPIC = 'control-GR-1'
CSV_FILE = 'data/stackexchange_users.csv'

config = {
    'bootstrap.servers': BROKER,
}

# Logging
logging.basicConfig(
    filename='struct-sst/kafka/producer.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

def create_topics():
    admin = AdminClient(config)

    for topic_name in [DATA_TOPIC, CONTROL_TOPIC]:
        try:
            topic = NewTopic(
                topic_name,
                num_partitions=3 if topic_name == DATA_TOPIC else 1,
                replication_factor=1
            )
            admin.create_topics([topic])
            print(f"Topic {topic_name} creado")
        except TopicAlreadyExistsError:
            print(f"Topic {topic_name} ya existe")

def delivery_report(err, msg):
    if err is not None:
        logging.error(f"Error al enviar mensaje: {err}")
    else:
        logging.debug(f"Mensaje enviado a {msg.topic()} [{msg.partition()}]")

def run_producer():
    create_topics()

    producer = Producer({'bootstrap.servers': BROKER})

    try:
        with open(CSV_FILE, newline='', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                producer.produce(
                    topic=DATA_TOPIC,
                    value=json.dumps(row).encode('utf-8'),
                    callback=delivery_report
                )
                if i % 100 == 0:
                    logging.info(f"{i} elementos enviados")
                producer.poll(0)

            # Avisa a los consumers de que ya no hay más mensajes
            producer.produce(
                topic=CONTROL_TOPIC,
                value=json.dumps("END").encode('utf-8'),
                callback=delivery_report
            )
            logging.info("Mensaje de fin enviado al topic de control")

            producer.flush()
            logging.info("Todos los mensajes han sido enviados y flush realizado")
    except FileNotFoundError:
        logging.error(f"El archivo {CSV_FILE} no se encontró.")
    except Exception as e:
        logging.error(f"Error al leer el archivo CSV: {e}")

if __name__ == '__main__':
    run_producer()

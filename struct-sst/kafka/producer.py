from confluent_kafka import Producer
import json
import time
import logging
import csv

# Configuración del logger
logger = logging.getLogger("PRODUCER")
logging.basicConfig(
    filename='kafka/producer.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# Configurar el productor Kafka
conf = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092'
}

producer = Producer(conf)

# Callback para manejar confirmaciones
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")
    else:
        logger.info(f"Message delivered to {msg.topic()} [{msg.partition()}]")

# Leer datos del CSV
with open("data/UsersSubsample.csv", "r", encoding="utf-8") as f:
    reader = csv.DictReader(f)
    items = list(reader)  # Convertimos el lector a una lista de diccionarios

# Enviar cada item al topic
for i, item in enumerate(items[:1000]):
    producer.produce("items-GR-1", json.dumps(item).encode("utf-8"), callback=delivery_report)
    if i % 100 == 0:
        logger.info(f"{i} items enviados.")
    producer.poll(0.1)  # Importante para hacer el envío

# Esperamos a que todos los mensajes se envíen
producer.flush()
logger.info("Todos los ítems enviados.")

# Enviar mensaje de parada al topic de control para cada consumidor
for i in range(3):
    producer.produce("control-GR-1", key=f"stop-{i}", value="STOP".encode("utf-8"), callback=delivery_report)
    producer.poll(0)
logger.info("Mensajes de parada enviados.")
producer.flush()

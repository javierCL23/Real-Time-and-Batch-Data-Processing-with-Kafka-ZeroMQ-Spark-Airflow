from confluent_kafka import Producer
import json
import time
import logging
import csv

# Configuración del logger
logger = logging.getLogger("PRODUCER")
logging.basicConfig(
    filename='struct-sst/kafka/producer_kafka.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# Configurar el productor Kafka con transactional.id
conf = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092',
    'transactional.id': 'producer-items-gr1'
}

producer = Producer(conf)

# Callback para manejar confirmaciones
def delivery_report(err, msg):
    if err is not None:
        logger.error(f"Message delivery failed: {err}")

# Inicializar transacciones
producer.init_transactions()

try:
    # Comenzar transacción
    producer.begin_transaction()

    # Leer datos del CSV
    with open("data/processed/UsersSubsample.csv", "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        items = list(reader)

    # Enviar datos
    for i, item in enumerate(items):
        producer.produce("items-GR-1", json.dumps(item).encode("utf-8"), callback=delivery_report)
        if (i+1) % 100 == 0:
            logger.info(f"{i+1} items sent.")
        producer.poll(0)

    # Enviar STOP al final (una vez que los datos están en la misma transacción)
    for i in range(3):
        producer.produce("control-GR-1", key=f"stop-{i}", value="STOP".encode("utf-8"), callback=delivery_report)

    # Confirmar toda la transacción
    producer.commit_transaction()
    logger.info("Todos los ítems y señales de STOP enviados atómicamente.")

except Exception as e:
    logger.error(f"Error en la transacción: {e}")
    producer.abort_transaction()
    logger.info("Transacción abortada por error.")

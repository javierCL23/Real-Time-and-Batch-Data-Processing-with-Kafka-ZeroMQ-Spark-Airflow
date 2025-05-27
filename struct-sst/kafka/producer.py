from confluent_kafka import Producer
import json
import logging
import csv
import toml

tml = toml.load("pyproject.toml")

# Configuración del logger
logger = logging.getLogger("PRODUCER")
logging.basicConfig(
    filename='struct-sst/kafka/producer_kafka.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
    )

# Configurar el productor Kafka con transactional.id
conf = {
    'bootstrap.servers': tml["tools"]["address"]["bootstrap.servers"],     #IP del server de Kafka con su puerto
    'transactional.id': 'producer-items-gr1'                    #Id del grupo de transacciones
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
    with open(tml["tools"]["project_paths"]["users_subsample_path"], "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        items = list(reader)

    # Enviar datos
    for i, item in enumerate(items):
        producer.produce(topic="items-GR-1", 
                         value=json.dumps(item).encode("utf-8"), 
                         partition = i%3, #Con esto nos aseguramos que cada partición tiene la misma cantidad de datos
                         callback=delivery_report)
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
    # Caso de error
    logger.error(f"Error en la transacción: {e}")
    producer.abort_transaction()
    logger.info("Transacción abortada por error.")

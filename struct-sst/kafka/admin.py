from confluent_kafka.admin import AdminClient, NewTopic
import time
import logging
import sys

# Configuración del logger
logger = logging.getLogger("ADMIN")
logging.basicConfig(
    filename='struct-sst/kafka/admin_kafka.log',
    #filename='admin_kafka.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)


config = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092', 
}

admin = AdminClient(config)

topics_to_reset = ["items-GR-1", "control-GR-1"]

# Eliminar los topic
logger.info("Deleting topics...")
delete_futures = admin.delete_topics(topics_to_reset, operation_timeout=30)

for topic, f in delete_futures.items():
    try:
        f.result()
        logger.info(f"Topic '{topic}' successfully deleted.")
    except Exception as e:
        logger.error(f"Failed to delete topic '{topic}': {e}")
        sys.exit(1)

# Esperamos un poco para que Kafka borre bien los topics

# Crear los topics de nuevo
logger.info("Creating topics...")
time.sleep(5)

new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics_to_reset]
create_futures = admin.create_topics(new_topics)

for topic, f in create_futures.items():
    try:
        f.result()
        logger.info(f"Topic '{topic}' successfully created.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic}': {e}")

logger.info("Admin program finished.")
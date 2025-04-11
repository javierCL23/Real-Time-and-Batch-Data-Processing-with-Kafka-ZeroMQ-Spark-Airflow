from confluent_kafka.admin import AdminClient, NewTopic
import time
import logging
import sys

# Configuración del logger
logger = logging.getLogger("ADMIN")
logging.basicConfig(
    filename='struct-sst/kafka/admin_kafka.log',
    #filename='admin_kafka.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
)

#Configuración de Kafka Admin
config = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092', #IP del server de Kafka con su puerto
}

admin = AdminClient(config)
topics_to_reset = ["items-GR-1", "control-GR-1"]


# Eliminar los topic
logger.info("Deleting topics...")
delete_futures = admin.delete_topics(topics_to_reset, operation_timeout=30)

#Comprobar que se han borrado de manera exitosa
for topic, f in delete_futures.items():
    try:
        f.result()
        logger.info(f"Topic '{topic}' successfully deleted.")
    except Exception as e:
        logger.error(f"Failed to delete topic '{topic}': {e}")
        sys.exit(1)

# Esperamos un poco para que Kafka borre bien los topics
time.sleep(5)

# Crear los topics de nuevo
logger.info("Creating topics...")

new_topics =[NewTopic(topics_to_reset[0], num_partitions=3, replication_factor=1), 
             NewTopic(topics_to_reset[1], num_partitions=1, replication_factor=1)]  #La partición del topic de control es mejor que sea 
                                                                                    #única para garantizar que todos ven su mensaje

create_futures = admin.create_topics(new_topics)

#Comprobar que se han creado de manera exitosa
for topic, f in create_futures.items():
    try:
        f.result()
        logger.info(f"Topic '{topic}' successfully created.")
    except Exception as e:
        logger.error(f"Failed to create topic '{topic}': {e}")

logger.info("Admin program finished.")
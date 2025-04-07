from confluent_kafka.admin import AdminClient, NewTopic
import time

config = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092', 
}

admin = AdminClient(config)

topics_to_reset = ["items-GR-1", "control-GR-1"]

# Eliminar los topics
print("Deleting topics...")
delete_futures = admin.delete_topics(topics_to_reset, operation_timeout=30)

for topic, f in delete_futures.items():
    try:
        f.result()
        print(f"Topic '{topic}' successfully deleted.")
    except Exception as e:
        print(f"Failed to delete topic '{topic}': {e}")

# Esperamos un poco para que Kafka borre bien los topics
time.sleep(5)

# Crear los topics de nuevo
print("\nCreating topics...")
new_topics = [NewTopic(topic, num_partitions=3, replication_factor=1) for topic in topics_to_reset]
create_futures = admin.create_topics(new_topics)

for topic, f in create_futures.items():
    try:
        f.result()
        print(f"Topic '{topic}' successfully created.")
    except Exception as e:
        print(f"Failed to create topic '{topic}': {e}")

print("\nAdmin program finished.")

from confluent_kafka import Consumer
import json
import logging
import sys
import zmq
import toml

import time
import json
import ujson
import pyarrow as pa
import pyarrow.parquet as pq

tml = toml.load("pyproject.toml")

#Configuración del log
if len(sys.argv) < 2 or sys.argv[1] not in ["0", "1", "2"]:
    #Solo los nombres 0,1,2 son válidos para el consumer
    raise ValueError("An argument between 0 and 2 is needed to identify the consumer.")

id = sys.argv[1]

processName = f"CONSUMER{id}"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename=f'struct-sst/kafka/consumer{id}_kafka.log',
    #filename=f'consumer{id}_kafka.log',
    filemode="w",       #Si existe el fichero, lo sobreescribe
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",   #Formato del mensaje
    datefmt="%Y-%m-%d %H:%M:%S",    #Formato del timestamp
    level=logging.INFO      #Puede informar con nivel info o superiores
)


# -------------------------------- ZMQ ------------------------------------------

context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5557")

# -------------------------------- Kafka ------------------------------------------
config = {
    'bootstrap.servers': tml["tools"]["address"]["bootstrap.servers"], #IP del server de Kafka con su puerto
    'group.id':          'consumers-kafka',             #Necesitan todos estar en mismo grupo para no solapar lecturas
    'enable.auto.commit': 'false',                      #En caso de error, los mensajes no se pierden
    'auto.offset.reset': 'earliest',                    #Si se usa latest puede que no procesen nada en caso de que los topics no se recarguen antes de volver a ejecutar consumersser.
    'isolation.level': 'read_committed',                #Necesario para el uso de transacciones
    'partition.assignment.strategy': 'roundrobin'       #Mejora el balance de carga
}

consumer = Consumer(config)

topic_msg = "items-GR-1"                        #Topic para procesamiento de los datos
topic_stop = "control-GR-1"                     #Topic para control de parada de procesamiento
consumer.subscribe([topic_msg, topic_stop])

logger.info(f"Consumer {id} ready to process data.")
results = {}
count = 0
still_data = True

while still_data:
        message = consumer.poll(1.0)
        if message is None:
            #No ve mensajes
            print("Waiting...")
        elif message.error():
            #Caso de error
            logging.error(f"ERROR: {message.error()}")
        else:
            #Si ya no quedan datos por procesar se debe parar
            if message.topic() == topic_stop:
                sender.send_json(results)
                logger.info(f"Ending processing in consumer {id}. {count} items processed.")
                #Confirmar que recibimos y procesamos el mensaje
                consumer.commit(message=message, asynchronous = False)
                #Salir del bucle
                still_data = False
            #Procesado de los datos
            else:
                #Cargamos el mensaje como un diccionario y lo procesamos
                data = json.loads(message.value())
                result = data['CreationDate'][:4]
                if result in results:
                    results[result] += 1
                else:
                    results[result] = 1
                #Confirmar que recibimos y procesamos el mensaje
                consumer.commit(message=message)
                count+=1
            #Logs
            if count%100 == 0 and still_data:
                #Cada 100 mensajes se informa de que se han procesado n mensajes
                logger.info(f"{count} items were processed.")
            

#Medición de tiempos con diferentes librerías:
#Los tiempos están almacenados en los logs.
start = time.time()
match id:
    case "0":   #JSON
        with open(tml["tools.project_paths"]["json_path"],"w") as f:
        #with open("JSON.json","w") as f:
            json.dump(dict(sorted(results.items())),f,indent=2)
        
        ending = time.time()
        logger.info(f"Elapsed time to write results with JSON : {ending-start}s.")
    case "1":   #UJSON
        with open(tml["tools.project_paths"]["ujson_path"],"w") as f:
        #with open("UJSON.json","w") as f:
            ujson.dump(dict(sorted(results.items())),f,indent=2)
        
        ending = time.time()
        logger.info(f"Elapsed time to write results with UJSON : {ending-start}s.")
    case "2":   #PARQUET
        years = list(results.keys())
        values = list(results.values())
        
        year_array = pa.array(years, pa.string())
        value_array = pa.array(values, pa.int32())
        
        table = pa.table([year_array, value_array], names=["year", "value"])
        start_write = time.time()
        pq.write_table(table, tml["tools.project_paths"]["parquet_path"])
        #pq.write_table(table, "PARQUET.parquet")
        ending = time.time()
        logger.info(f"Elapsed time to write results with PARQUET : {ending-start}s ({ending-start_write}s real time).")
    case _:
        #Nunca llegará aquí, pero por seguridad
        print(f"ERROR: id:{id} is not a valid option.")


#El más rápido es ujson dado el tamaño de los datos, pero es posible que a medida que escale el tamaño de datos a escribir, parquet sea mejor.
#El más lento con diferencia es json

#Cerramos comunicaciones en ZMQ y Kafka 
consumer.unsubscribe()
consumer.close()
logger.info(f"Consumer {id} has unsubscribe")

sender.close()
logger.info(f"Consumer {id} has close comunication from ZMQ")

logger.info(f"Consumer {id} Finished")

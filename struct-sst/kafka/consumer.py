from confluent_kafka import Consumer
import json
import logging
import sys 
import zmq

import time
import json
import ujson
import pyarrow as pa
import pyarrow.parquet as pq

if len(sys.argv) < 2:
    if (sys.argv[1] not in ["0","1","2"]):
        raise ValueError("An argument between 0 and 2 is nedeed to identify the consumer.")

id = sys.argv[1]

processName = f"CONSUMER{id}"
logger = logging.getLogger(processName)
logging.basicConfig(
    filename=f'struct-sst/kafka/consumer{id}.log',
    filemode="w",
    format="|%(name)s|[%(asctime)s.%(msecs)04d]:%(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    level=logging.INFO
)

# -------------------------------- ZMQ ------------------------------------------

context = zmq.Context()
sender = context.socket(zmq.PUSH)
sender.connect("tcp://localhost:5557")

# -------------------------------- Kafka ------------------------------------------
config = {
    'bootstrap.servers': 'docker01.aulas.eif.urjc.es:9092',
    'group.id':          'consumers-kafka',             #Necesitan todos estar en mismo grupo para no solapar lecturas
    'enable.auto.commit': 'false',                      #En caso de error, los mensajes no se pierden
    'auto.offset.reset': 'latest'                       #Como no se procesarán los mensajes repetidas veces no es necesario tener earliest
}

consumer = Consumer(config)

topic_msg = "items-GR-1"                        #Topic para procesamiento de los datos
topic_stop = "stop"  #Topic para control de parada de procesamiento
consumer.subscribe([topic_msg, topic_stop])


results = dict()
count = 0
still_data = True

while still_data:
        message = consumer.poll(1.0)
        if message.error():
            logging.error(f"ERROR: {message.error()}")
        else:
            #Si ya no quedan datos por procesar se debe parar
            if message.topic() == topic_stop:
                sender.send_json(json.dumps(results))
                logger.info(f"Ending processing in worker{id}. {count} items processed.")
                still_data = False
            #Procesado de los datos
            else:
                data = json.loads(message.value())
                result = data['CreationDate'][:4]
                if result in results:
                    results[result] += 1
                else:
                    results[result] = 1
                consumer.commit(message=message)
                count+=1
            #Logs
            if count%100 == 0:
                logger.info(f"{count} items were processed.")
            
#Cerramos comunicaciones en ZMQ y Kafka
consumer.unsubscribe()
consumer.close()
context.term()

#Medición de tiempos con diferentes librerías:
start = time.time()
match id:
    case "0":   #JSON
        with open("JSON.json","w") as f:
            json.dump(results,f)
        
        ending = time.time()
        logger.info(f"Elapsed time to write results with JSON : {ending-start}s.")
    case "1":   #UJSON
        with open("UJSON.json","w") as f:
            ujson.dump(results,f)
        
        ending = time.time()
        logger.info(f"Elapsed time to write results with UJSON : {ending-start}s.")
    case "2":   #PARQUET
        years = list(results.keys())
        values = list(results.values())
        
        year_array = pa.array(years, pa.string())
        value_array = pa.array(values, pa.int32())
        
        table = pa.table([year_array, value_array], names=["year", "value"])
        start_write = time.time()
        pq.write_table(table, "PARQUET.parquet")
        
        ending = time.time()
        logger.info(f"Elapsed time to write results with PARQUET : {ending-start}s ({start_write}s real time).")
    case _:
        print(f"ERROR: id:{id} is not a valid option.")





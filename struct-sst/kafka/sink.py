import zmq
import json
import logging

logging.basicConfig(filename='sink.log', level=logging.INFO)

ctx = zmq.Context()
socket = ctx.socket(zmq.PULL)
socket.bind("tcp://*:5557")

final_results = {}

for _ in range(3):  # Esperamos 3 consumidores
    message = socket.recv_json()
    consumer_id = message['consumer']
    results = message['results']
    for year, count in results.items():
        final_results[year] = final_results.get(year, 0) + count
    logging.info(f"Recibido de consumer {consumer_id}: {results}")

# Escribir resultados totales
with open('final-results-kafka.txt', 'w') as f:
    for year in sorted(final_results):
        f.write(f"{year}: {final_results[year]}\n")

logging.info("Sink ha terminado de escribir los resultados")
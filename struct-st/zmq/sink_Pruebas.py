import logging
import sys
import time
import zmq
from random import randint

logging.basicConfig(
        filename='pruebas.log',
        format="[{asctime}]:{levelname} - {message}",
        style="{", 
        datefmt="%Y-%m-%d %H:%M")
logger = logging.getLogger("prueba")
logger.setLevel(logging.INFO)

logger.info('Started')

datos = [str(randint(1,20)) for _ in range(50)]

resultados = {}


for dato in datos:
    if dato in resultados:
        resultados[dato]+=1
        logger.info("Valor encontrado anteriormente, sumando 1")
    else:
        resultados[dato] = 1
        logger.warning("Valor no encontrado anteriormente, añadiendo por primera vez")
print(resultados)

logger.info('Finished')

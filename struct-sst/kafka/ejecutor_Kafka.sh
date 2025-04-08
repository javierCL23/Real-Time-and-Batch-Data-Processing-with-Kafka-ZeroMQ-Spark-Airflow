#!/bin/sh


#Configuración

SCRIPT_DIR="struct-sst/kafka"
#SCRIPT_DIR="."
LOG_DIR="struct-sst/kafka"
#LOG_DIR="."
FINAL_LOG="Kafka_log.log"

#Limpieza inicial
rm -f $LOG_DIR/*.log 2>/dev/null

#Función para matar posibles remanentes entre ejecuciones
cleanup() {
    echo "Terminando procesos..."
    pkill -f "python.*(sink.py|consumer.py|producer.py|admin.py)" 2>/dev/null
}
trap cleanup EXIT INT TERM

#Iniciar Admin
echo "Iniciando Admin"

python3 $SCRIPT_DIR/admin.py
ADMIN_EXIT=$?
if [ $ADMIN_EXIT -ne 0 ]; then
    exit 1
fi

sleep 2 #Algo más de tiempo para que terminen de mandarse aparte del que da el propio admin
echo Admin ha terminado

python3 $SCRIPT_DIR/sink.py  &
SINK_PID=$!


python3 $SCRIPT_DIR/consumer.py 0 > /dev/null &
WORKER0_PID=$!
python3 $SCRIPT_DIR/consumer.py 1 > /dev/null &
WORKER1_PID=$!
python3 $SCRIPT_DIR/consumer.py 2 > /dev/null &
WORKER2_PID=$!

python3 $SCRIPT_DIR/producer.py &

#Solo esperamos a este porque es el último en terminar siempre
echo "Esperando a que acaben todos de ejecutar"
wait $SINK_PID $WORKER0_PID $WORKER1_PID $WORKER2_PID

# 5. Unificar logs
# Esto lo conseguimos con awk y sort. Awk se encarga de usar el siguente formato:   "TIMESTAMP, ID, Linea Relativa en su log propio, mensaje"
#En el sort se ordena por el time stamp y en caso de empate se ordena usando el Id y linea relativa, lo que nos asegura que el orden original se mantenga
echo "Unificando logs..."
awk -F'[][]' '{print $2 "," $1 "," FNR "," $0}' $LOG_DIR/*_kafka.log | sort -t',' -k1,1n | cut -d "," -f4- > $LOG_DIR/$FINAL_LOG
rm -f $LOG_DIR/*_kafka.log

echo "Proceso completado. Log unificado: $LOG_DIR/$FINAL_LOG"

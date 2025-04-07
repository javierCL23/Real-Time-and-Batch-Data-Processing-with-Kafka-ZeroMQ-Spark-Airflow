#!/bin/sh


#Configuración

SCRIPT_DIR="struct-sst/kafka"
#SCRIPT_DIR="."
LOG_DIR="struct-sst/kafka"
#LOG_DIR="."
FINAL_LOG="Kafka_log.log"

#Limpieza inicial
rm -f $LOG_DIR/*.log 2>/dev/null

cleanup() {
    echo "Terminando procesos..."
    pkill -f "python.*(sink.py|consumer.py|producer.py|admin.py)" 2>/dev/null
}
trap cleanup EXIT INT TERM

#Iniciar Admin
python3 $SCRIPT_DIR/admin.py
ADMIN_EXIT=$?
if [ $ADMIN_EXIT -ne 0 ]; then
    exit 1
fi

for i in $(seq 0 2); do
    python3 $SCRIPT_DIR/consumer.py $i &
done

python3 $SCRIPT_DIR/sink.py &
SINK_PID=$!

python3 $SCRIPT_DIR/producer.py &

#Solo esperamos a este porque es el último en terminar siempre
echo "Esperando a que acabe el sink de ejecutar"
wait $SINK_PID

# 5. Unificar logs
echo "Unificando logs..."
awk -F'[][]' '{print $2 "," FNR "," $0}' $LOG_DIR/*_kafka.log | sort -t',' -k1,1n | cut -d "," -f3- > $LOG_DIR/$FINAL_LOG
rm -f *_kafka.log

echo "Proceso completado. Log unificado: $LOG_DIR/$FINAL_LOG"

!/bin/bash

# Configuración
NUM_WORKERS=3                     #Modificable (solución generalizada a n workers)
SCRIPT_DIR="struct-sst/zmq"       # Directorio de los scripts
LOG_DIR="struct-sst/zmq"          # Directorio de logs
FINAL_LOG="ZMQ_log.log"           # Nombre del log unificado

# Limpieza inicial
rm -f $LOG_DIR/*.log 2>/dev/null

# Función de limpieza
cleanup() {
    echo "Terminando procesos..."
    pkill -f "python.*(sink.py|worker.py|ventilator.py)" 2>/dev/null
}
#En caso de hacer Ctrl+C, mata todos los procesos en segundo plano.
trap cleanup EXIT INT TERM

# 1. Iniciar Sink
echo "Iniciando Sink..."
python3 $SCRIPT_DIR/sink.py &
SINK_PID=$!

# 2. Iniciar Workers
echo "Iniciando $NUM_WORKERS workers..."
for i in $(seq 1 $NUM_WORKERS); do
    python3 $SCRIPT_DIR/worker.py $i &
done

# 3. Iniciar Ventilator
echo "Iniciando Ventilator..."
python3 $SCRIPT_DIR/ventilator.py

# 4. Esperar finalización del sink
echo "Esperando finalización del pipeline..."
wait $SINK_PID

# 5. Unificar logs
# Esto lo conseguimos con awk y sort. Awk se encarga de usar el siguente formato:   "TIMESTAMP, ID, Linea Relativa en su log propio, mensaje"
# En el sort se ordena por el time stamp y en caso de empate se ordena usando el Id y linea relativa, lo que nos asegura que el orden original se mantenga
echo "Unificando logs..."
awk -F'[][]' '{print $2 "," $1 "," FNR "," $0}' $LOG_DIR/*_zmq.log | sort -t',' -k1,1V -k2,2 -k3,3n | cut -d "," -f4- > $LOG_DIR/$FINAL_LOG

#rm -f $LOG_DIR/ventilador_zmq.log $LOG_DIR/sink_zmq.log $LOG_DIR/worker*_zmq.log

echo "Proceso completado. Log unificado: $LOG_DIR/$FINAL_LOG"

# Sistemas Distribuidos de Procesamiento de Datos II

**DEFENSA FINAL - Grado en Ciencia e Ingeniería de Datos**

## 👥 Equipo de Desarrollo

- **Javier Carreño Luque**
- **Carlos Arévalo López**  
- **Miguel Alcocer Pérez**

**Fecha de entrega:** 26 de Mayo de 2025

---

## 📋 Resumen Ejecutivo

Este proyecto implementa tres pipelines principales que abordan diferentes aspectos del procesamiento distribuido de datos:

1. **ETL con Apache Airflow** - Extracción, transformación y carga de datos de StackExchange
2. **Mensajería Distribuida** - Procesamiento con Apache Kafka y ZeroMQ
3. **Streaming en Tiempo Real** - Procesamiento continuo con Spark Structured Streaming

Los resultados demuestran la efectividad de las tecnologías distribuidas para el manejo de grandes volúmenes de datos, procesando más de 1000 registros de usuarios y posts, mejorando significativamente el rendimiento mediante distribución de carga, y alcanzando procesamiento en tiempo real con latencias mínimas.

---

## 🏗️ Estructura del Proyecto

```
proyecto/
├── practica1/
│   ├── ETL_ConSQL.py          # Pipeline ETL principal
│   ├── data/                  # Datos procesados y datasets
│   └── config/                # Configuraciones del proyecto
├── practica2/
│   ├── kafka/
│   │   ├── admin.py          # Administración de topics Kafka
│   │   ├── producer.py       # Productor de mensajes
│   │   ├── consumer.py       # Consumidor de mensajes
│   │   └── sink.py           # Recolector de resultados
│   └── zmq/
│       ├── ventilator.py     # Distribuidor de tareas ZeroMQ
│       ├── worker.py         # Procesador de tareas
│       └── sink.py           # Consolidador de resultados
├── practica2-fase2/
│   └── spark-streaming/      # Implementación Spark Structured Streaming
└── README.md
```

---

## 🚀 Tecnologías Utilizadas

### Práctica 1 - Pipeline ETL
- **Apache Airflow** - Orquestación de flujos de trabajo
- **Pandas** - Manipulación y análisis de datos
- **SQLite** - Base de datos relacional
- **BeautifulSoup** - Extracción de texto HTML
- **Py7zr** - Descompresión de archivos
- **Plotly** - Visualización interactiva
- **Scikit-learn** - División de datasets

### Práctica 2 - Mensajería Distribuida
- **Apache Kafka** - Streaming distribuido
- **ZeroMQ** - Mensajería de alto rendimiento
- **JSON/UJSON/Parquet** - Formatos de serialización

### Práctica 2 Fase 2 - Streaming
- **Apache Spark** - Procesamiento distribuido
- **Spark Structured Streaming** - Streaming en tiempo real
- **Kafka Integration** - Conectividad con Kafka

---

## 📊 Componentes Principales

### 🔄 Práctica 1: Pipeline ETL con Apache Airflow

**Objetivo:** Procesamiento batch de datos de StackExchange con transformaciones complejas.

**Tareas del DAG:**
- `GetData` - Descarga y descompresión de datos XML
- `RemoveNull` - Limpieza de valores nulos
- `TransformWebsiteURL` - Conversión a valores binarios
- `ConvertDates` - Transformación de fechas
- `HTML_to_Text` - Extracción de texto desde HTML
- `Country_Location` - Asignación geográfica
- `Parse_Tags` - Procesamiento de etiquetas
- `LoadData` - Almacenamiento en CSV
- `LoadGraph` - Generación de visualizaciones

**Resultados:**
- ✅ Datasets limpios y estructurados
- ✅ Visualización interactiva de usuarios por país
- ✅ Base de datos SQLite con conjuntos de entrenamiento/prueba
- ✅ Transformaciones aplicadas correctamente

### 📨 Práctica 2: Mensajería Distribuida

**Arquitectura Kafka:**
- **Admin** - Gestión de topics
- **Producer** - Envío transaccional de datos
- **Consumer** - Procesamiento con múltiples librerías
- **Sink** - Consolidación de resultados

**Configuración de Topics:**
- `items-GR-1` - Procesamiento de datos (3 particiones)
- `control-GR-1` - Control de parada (1 partición)

**Arquitectura ZeroMQ:**
- **Patrón Ventilator-Worker-Sink**
- Distribución eficiente de tareas
- Procesamiento paralelo optimizado

**Comparativa de Rendimiento:**
| Tecnología | Complejidad | Persistencia | Escalabilidad | Latencia |
|------------|-------------|--------------|---------------|----------|
| Kafka      | Alta        | Sí           | Excelente     | Media    |
| ZeroMQ     | Baja        | No           | Buena         | Baja     |

### 🌊 Práctica 2 Fase 2: Spark Structured Streaming

**Objetivo:** Procesamiento de datos en tiempo real con integración Kafka-Spark.

**Componentes:**
- **Productor Kafka** - Alimentación del topic `purchases`
- **Consumidor Spark** - Procesamiento streaming continuo
- **Análisis en tiempo real** - Métricas y agregaciones

**Configuración:**
- Servidor: `docker01.aulas.eif.urjc.es:9092`
- StartingOffsets: `earliest` para procesamiento completo
- Output: In-memory table para consultas directas

---

## 🏛️ Diseño de Pipeline ML con Streaming

### Arquitectura Propuesta

```
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   Apache Kafka  │───▶│ Spark Structured     │───▶│ Preprocessing   │
│                 │    │ Streaming            │    │                 │
└─────────────────┘    └──────────────────────┘    └─────────────────┘
                                  │                           │
                                  ▼                           ▼
┌─────────────────┐    ┌──────────────────────┐    ┌─────────────────┐
│   Output Sink   │◀───│    ML Model          │◀───│ Feature         │
│                 │    │                      │    │ Engineering     │
└─────────────────┘    └──────────────────────┘    └─────────────────┘
```

### Componentes del Sistema

**1. Ingesta de Datos (Apache Kafka)**
- Topic: `stackexchange-stream`
- Particionamiento por tipo de contenido
- Serialización eficiente (Avro/Protocol Buffers)

**2. Procesamiento Streaming (Spark)**
- Lectura continua desde Kafka
- Ventanas temporales para agregaciones
- Checkpointing para tolerancia a fallos

**3. Preprocesamiento**
- Limpieza de datos HTML
- Normalización de fechas y ubicaciones
- Feature engineering en tiempo real

**4. Modelo ML**
- Predicción de categorías de posts
- Actualización online del modelo
- Métricas de rendimiento en tiempo real

---

## 🔧 Instalación y Configuración

### Prerrequisitos

```bash
# Python 3.8+
pip install apache-airflow pandas sqlite3 beautifulsoup4 py7zr plotly scikit-learn

# Kafka
pip install kafka-python

# ZeroMQ
pip install pyzmq

# Spark
pip install pyspark
```

### Variables de Entorno

```bash
export AIRFLOW_HOME=~/airflow
export KAFKA_BROKER=docker01.aulas.eif.urjc.es:9092
export SPARK_HOME=/path/to/spark
```

---

## 🚀 Instrucciones de Ejecución

### Práctica 1 - Pipeline ETL

```bash
# Inicializar Airflow
airflow db init
airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com

# Ejecutar DAG
airflow dags trigger ETL_ConSQL
```

### Práctica 2 - Kafka

```bash
# Terminal 1: Administrador
python kafka/admin.py

# Terminal 2: Productor
python kafka/producer.py

# Terminal 3: Consumidor
python kafka/consumer.py

# Terminal 4: Sink
python kafka/sink.py
```

### Práctica 2 - ZeroMQ

```bash
# Terminal 1: Sink
python zmq/sink.py

# Terminal 2: Ventilator
python zmq/ventilator.py

# Terminal 3-N: Workers
python zmq/worker.py
```

### Práctica 2 Fase 2 - Spark Streaming

```bash
# Ejecutar aplicación Spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark-streaming/app.py
```

---

## 📈 Resultados y Métricas

### Rendimiento ETL
- **Registros procesados:** 1000+ usuarios y posts
- **Tiempo de ejecución:** ~15 minutos
- **Transformaciones exitosas:** 100%

### Mensajería Distribuida
- **Throughput Kafka:** ~1000 msg/seg
- **Latencia ZeroMQ:** <10ms
- **Eficiencia UJSON:** Mejor rendimiento para datasets medianos

### Streaming
- **Latencia de procesamiento:** <1 segundo
- **Tolerancia a fallos:** Checkpointing activo
- **Escalabilidad:** Horizontal automática

---

## 🛠️ Configuraciones Avanzadas

### Kafka Consumer Configuration

```python
consumer_config = {
    'bootstrap_servers': 'docker01.aulas.eif.urjc.es:9092',
    'group_id': 'GR-1',
    'auto_offset_reset': 'earliest',
    'enable_auto_commit': True
}
```

### Spark Streaming Configuration

```python
spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "docker01.aulas.eif.urjc.es:9092") \
    .option("subscribe", "purchases") \
    .option("startingOffsets", "earliest") \
    .load()
```

---

## 🔍 Monitoreo y Debugging

### Logs de Airflow
```bash
tail -f $AIRFLOW_HOME/logs/dag_id/task_id/execution_date/1.log
```

### Kafka Consumer Lag
```bash
kafka-consumer-groups.sh --bootstrap-server docker01.aulas.eif.urjc.es:9092 --describe --group GR-1
```

### Spark Streaming UI
```
http://localhost:4040
```

---

## 🤝 Contribuciones

Cada miembro del equipo contribuyó de manera equitativa:

- **Javier Carreño Luque:** Pipeline ETL y documentación
- **Carlos Arévalo López:** Sistemas de mensajería distribuida  
- **Miguel Alcocer Pérez:** Spark Structured Streaming y arquitectura ML

---

## 📚 Referencias

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html)
- [Spark Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Spark ML Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [ZeroMQ Guide](https://zguide.zeromq.org/)

---

## 📞 Contacto

Para consultas sobre el proyecto, contactar con cualquier miembro del equipo a través de la plataforma GitLab EIF de la URJC.

---

**Universidad Rey Juan Carlos - EIF + ETSII**  
**Grado en Ciencia e Ingeniería de Datos**  
**Curso 2024/2025**
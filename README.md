# Sistemas Distribuidos de Procesamiento de Datos II

**DEFENSA FINAL - Grado en Ciencia e Ingeniería de Datos**

## 👥 Equipo de Desarrollo

- **Javier Carreño Luque**
- **Carlos Arévalo López**  
- **Miguel Alcocer Pérez**

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
├── data/                      # Datos procesados y en crudo además de algún que otro extra
├── src/                       # Códigos útiles usados en el proyecto
├── notebooks/                 # Jupyter Notebooks con el EDA para las decisiones del DAG de airflow
├── stack_exchange_eda/dags/
│   ├── ETL_ConSQL.py          # Pipeline ETL principal
│   └── config/                # Configuraciones del proyecto
├── struct-sst/
│   ├── kafka/
│   │   ├── admin.py          # Administración de topics Kafka
│   │   ├── producer.py       # Productor de mensajes
│   │   ├── consumer.py       # Consumidor de mensajes
│   │   └── sink.py           # Recolector de resultados
│   └── zmq/
│       ├── ventilator.py     # Distribuidor de tareas ZeroMQ
│       ├── worker.py         # Procesador de tareas
│       └── sink.py           # Consolidador de resultados
├── spark-streaming/          # Implementación Spark Structured Streaming
├── pyproject.toml            # Archivo con los prerequisitos y variables de configuración del proyecto
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

## 🔧 Instalación y Configuración

### Prerrequisitos

```bash
uv venv --python 3.11
source .venv/bin/activate
uv pip install .
```

Con esto se creará el entorno virtual con python 3.11 y se instalarán todas las dependencias necesarias.

---

## 🚀 Instrucciones de Ejecución (todas con el entorno iniciado)

### Práctica 1 - Pipeline ETL

```bash
# Iniciar Airflow
airflowctl init stack_exchange_eda #Para crear el proyecto de airflow

#Una vez creado se debe de cambiar la ruta del entorno virtual que usa en 
#el settings.yaml para que sea la del proyecto general

airflow start stack_exchange
```
Desde un buscador web acceder a http://localhost:8080/ con usuario admin, contraseña, la ofrecida en el start

### Práctica 2 - Kafka

```bash
./struct-sst/kafka/ejecutor_Kafka.sh
```

### Práctica 2 - ZeroMQ

```bash
./struct-sst/kafka/ejecutor_Kafka.sh
```

### Práctica 2 Fase 2 - Spark Streaming

```bash
# Ejecutar versión local
python3 spark-streaming/kafka-producer-confluent.py #Para producir mensajes
python3 spark-streaming/kafka-consumer-confluent.py #Para consumir los mensajes del producer

# Ejecutar aplicación Spark
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 spark-streaming/struct_kafka_consumer_local.py
```

---

## 📈 Resultados y Métricas

### Rendimiento ETL
- **Registros procesados:** 500k+ registros entre usuarios y posts
- **Tiempo de ejecución:** ~3 minutos
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

## 🔍 Monitoreo y Debugging

### Logs de Airflow
```bash
#Rellenar con lo que se quiera monitorear
tail -f stack_exchange_eda/logs/"dag_id=etl_dag"/"EJECUCION_CONCRETA"/"TASK_ID"/"LOG_CONCRETO"
```
También se pueden ver desde http://localhost:8080/dags/etl_dag/grid?tab=logs

### Spark Streaming UI
```
http://localhost:4040
```

---

## 📚 Referencias

- [Apache Airflow Documentation](https://airflow.apache.org/docs/)
- [Apache Kafka Documentation](https://kafka.apache.org/documentation/)
- [Spark Structured Streaming Guide](https://spark.apache.org/docs/3.5.1/structured-streaming-programming-guide.html)
- [Spark Kafka Integration](https://spark.apache.org/docs/latest/structured-streaming-kafka-integration.html)
- [Spark ML Guide](https://spark.apache.org/docs/latest/ml-guide.html)
- [ZeroMQ Guide](https://zguide.zeromq.org/)

---

**Universidad Rey Juan Carlos - EIF + ETSII**  
**Grado en Ciencia e Ingeniería de Datos**  
**Curso 2024/2025**
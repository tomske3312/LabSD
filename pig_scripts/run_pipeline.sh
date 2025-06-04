#!/bin/bash

# Activar el modo de depuración para ver cada comando ejecutado y sus resultados
set -x
# Salir inmediatamente si un comando falla
set -e

echo "Iniciando run_pipeline.sh..."

# 1. Esperar a que Hadoop Namenode salga del modo seguro (safemode)
echo "Esperando que Hadoop Namenode salga de safemode..."
until hdfs dfsadmin -safemode get | grep -q 'OFF'; do
  echo "Namenode aún en safemode, esperando..."
  sleep 5
done
echo "Hadoop Namenode está listo."

# 2. Esperar a que MongoDB tenga eventos para procesar
# El importador debería estar llenando MongoDB. Esperamos un mínimo de eventos.
echo "Confiando en que mongo_importer está llenando MongoDB..."
# Dar tiempo a que el importador procese los primeros archivos.
# El importador ya tiene su propia lógica de espera y umbral.
sleep 60 # Aumentado a 60 segundos para darle más tiempo al scraper/importer

# Configurar el classpath de Pig para los JARs de MongoDB y JSON Simple.
# Estas variables (PIG_VERSION, MONGO_HADOOP_VERSION) son pasadas como ENVIRONMENT del servicio
# 'pig-runner' en docker-compose.yml
export PIG_CLASSPATH=/opt/pig-${PIG_VERSION}/lib/mongo-hadoop-core-${MONGO_HADOOP_VERSION}.jar:/opt/pig-${PIG_VERSION}/lib/mongo-hadoop-pig-${MONGO_HADOOP_VERSION}.jar:/opt/pig-${PIG_VERSION}/lib/mongo-java-driver-3.12.11.jar:/opt/pig-${PIG_VERSION}/lib/json-simple-1.1.1.jar

echo "PIG_CLASSPATH configurado: $PIG_CLASSPATH"

# 3. Exportar datos de MongoDB a HDFS
echo "Ejecutando script de exportación de MongoDB a HDFS..."
# Se pasan las variables de entorno MONGO_HOST y REDIS_HOST directamente en docker-compose.yml
python3 /scripts_auxiliares/export_mongo_to_hdfs.py
if [ $? -ne 0 ]; then
  echo "Error: export_mongo_to_hdfs.py falló. Abortando."
  exit 1
fi
echo "Exportación de MongoDB a HDFS completada."

# Asegurarse que los directorios de salida de Pig existan en HDFS
hdfs dfs -mkdir -p /user/hadoop/waze_processed
echo "Directorio HDFS /user/hadoop/waze_processed asegurado."
hdfs dfs -mkdir -p /user/hadoop/waze_analysis
echo "Directorio HDFS /user/hadoop/waze_analysis asegurado."

# 3.1. Limpiar directorios de salida de Pig antes de cada ejecución para idempotencia
echo "Limpiando directorios de salida de Pig en HDFS..."
hdfs dfs -rm -r -f /user/hadoop/waze_processed/filtered_homogenized_events || true # '|| true' para que no falle si no existe
hdfs dfs -rm -r -f /user/hadoop/waze_analysis/commune_summary.json || true
hdfs dfs -rm -r -f /user/hadoop/waze_analysis/type_summary.json || true
hdfs dfs -rm -r -f /user/hadoop/waze_analysis/daily_summary.json || true
hdfs dfs -rm -r -f /user/hadoop/waze_analysis/hourly_summary.json || true
echo "Directorios de salida de Pig limpiados."

# 4. Ejecutar el script de filtrado y homogeneización de Pig (leerá de HDFS, escribirá a HDFS)
echo "Ejecutando script Pig: 01_filter_homogenize.pig..."
pig -f /pig_scripts/01_filter_homogenize.pig 2>&1 | tee /tmp/pig_filter_homogenize.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "Error: 01_filter_homogenize.pig falló. Ver log en /tmp/pig_filter_homogenize.log o en la consola."
  exit 1
fi
echo "01_filter_homogenize.pig completado."

# 5. Ejecutar el script de análisis de datos de Pig (leerá de HDFS, escribirá a HDFS)
echo "Ejecutando script Pig: 02_analyze_data.pig..."
pig -f /pig_scripts/02_analyze_data.pig 2>&1 | tee /tmp/pig_analyze_data.log
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "Error: 02_analyze_data.pig falló. Ver log en /tmp/pig_analyze_data.log o en la consola."
  exit 1
fi
echo "02_analyze_data.pig completado."

# 6. Cargar resultados de HDFS a Redis
echo "Ejecutando script de carga de resultados de Pig a Redis..."
# Las variables MONGO_HOST y REDIS_HOST se pasan a este script por defecto en docker-compose
python3 /scripts_auxiliares/load_pig_results_to_redis.py
if [ $? -ne 0 ]; then
  echo "Error: load_pig_results_to_redis.py falló. Abortando."
  exit 1
fi
echo "Carga de resultados a Redis completada."

echo "Pipeline de procesamiento de Pig completado exitosamente."

# Mantener el contenedor corriendo indefinidamente para que Docker Compose no lo detenga
tail -f /dev/null

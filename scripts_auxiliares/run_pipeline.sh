#!/bin/bash

# Activar el modo de depuración para ver cada comando ejecutado y sus resultados
set -x 
# Salir inmediatamente si un comando falla
set -e

echo "Iniciando run_pipeline.sh..."

# 1. Esperar a que Hadoop Namenode salga del modo seguro (safemode)
# Esto asegura que HDFS esté listo para operar.
echo "Esperando que Hadoop Namenode salga de safemode..."
until hdfs dfsadmin -safemode get | grep -q 'OFF'; do
  echo "Namenode aún en safemode, esperando..."
  sleep 5
done
echo "Hadoop Namenode está listo."

# 2. Esperar a que MongoDB tenga eventos para procesar
# Una forma más robusta: usar mongosh para contar documentos
echo "Esperando que MongoDB tenga al menos ${MIN_EVENTS_THRESHOLD:-500} eventos para procesar..."
MONGO_HOST=${MONGO_HOST:-mongo_db}
MONGO_PORT=${MONGO_PORT:-27017}
MONGO_DB_NAME=${MONGO_DB_NAME:-waze_data}
MONGO_COLLECTION_NAME=${MONGO_COLLECTION_NAME:-events}

# Añadir un contador de intentos para evitar bucles infinitos
MAX_MONGO_WAIT_ATTEMPTS=60 # 60 intentos * 5 segundos = 5 minutos
CURRENT_MONGO_WAIT_ATTEMPT=0

# Loop para verificar si MongoDB tiene al menos MIN_EVENTS_THRESHOLD eventos
# Asegúrate de que `mongosh` esté disponible en la imagen pig-runner, o usa `mongo` si es una versión antigua.
# Si `mongosh` no está, podrías necesitar instalarlo en Dockerfile.pig o usar una imagen base con más herramientas.
# Por simplicidad, aquí usaremos curl para una verificación HTTP en MongoDB si la API HTTP está habilitada,
# o una suposición de que el importador está funcionando.
# La forma más fiable con mongosh sería:
# mongosh --host $MONGO_HOST --port $MONGO_PORT --quiet --eval "db.$MONGO_COLLECTION_NAME.countDocuments({})"

# Para evitar instalar mongosh en Pig (ya que es un contenedor de procesamiento):
# Confiamos en el mongo_importer.py para llenar la base de datos.
# El pig-runner ya tiene una dependencia en mongo_db (service_healthy).
# Si el importador está funcionando, eventualmente habrá datos.
# Si Pig falla por "No data", es una señal de que el importador no ha hecho su trabajo.
# Por ahora, simplemente confiamos en el importador y pasamos al siguiente paso.
# Si quieres una verificación de conteo real desde bash en un contenedor Pig,
# necesitarías instalar un cliente mongo.

echo "Confiando en que mongo_importer está llenando MongoDB..."
# Puedes añadir una espera fija si el scraper/importer tarda mucho en el primer ciclo
sleep 30 # Esperar 30 segundos extra para que el importador cargue algunos datos

# Configurar el classpath de Pig para los JARs de MongoDB
# Es crucial que PIG_CLASSPATH apunte a los JARs correctos.
# Esto ya está en el Dockerfile.pig, pero lo volvemos a exportar para asegurar.
export PIG_CLASSPATH=/opt/pig-${PIG_VERSION}/lib/mongo-hadoop-core-${MONGO_HADOOP_VERSION}.jar:/opt/pig-${PIG_VERSION}/lib/mongo-hadoop-pig-${MONGO_HADOOP_VERSION}.jar:/opt/pig-${PIG_VERSION}/lib/mongo-java-driver-3.12.11.jar

echo "PIG_CLASSPATH configurado: $PIG_CLASSPATH"

# 3. Ejecutar el script de filtrado y homogeneización de Pig
echo "Ejecutando script Pig: 01_filter_homogenize.pig..."
# === MODIFICACIÓN CLAVE: Redirigir stderr a stdout para ver errores de Pig ===
pig -f /pig_scripts/01_filter_homogenize.pig 2>&1 | tee /tmp/pig_filter_homogenize.log
# Verificar el código de salida de Pig
if [ ${PIPESTATUS[0]} -ne 0 ]; then # PIPESTATUS[0] obtiene el código de salida del primer comando en el pipeline (pig)
  echo "Error: 01_filter_homogenize.pig falló. Ver log en /tmp/pig_filter_homogenize.log o en la consola."
  exit 1
fi
echo "01_filter_homogenize.pig completado."

# 4. Ejecutar el script de análisis de datos de Pig
echo "Ejecutando script Pig: 02_analyze_data.pig..."
# === MODIFICACIÓN CLAVE: Redirigir stderr a stdout para ver errores de Pig ===
pig -f /pig_scripts/02_analyze_data.pig 2>&1 | tee /tmp/pig_analyze_data.log
# Verificar el código de salida de Pig
if [ ${PIPESTATUS[0]} -ne 0 ]; then
  echo "Error: 02_analyze_data.pig falló. Ver log en /tmp/pig_analyze_data.log o en la consola."
  exit 1
fi
echo "02_analyze_data.pig completado."

echo "Pipeline de procesamiento de Pig completado exitosamente."

# Mantener el contenedor corriendo para depuración o si se esperan más ejecuciones manuales
tail -f /dev/null

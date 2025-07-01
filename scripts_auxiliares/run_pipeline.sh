#!/bin/bash
set -ex

echo "--- Iniciando Pipeline de Procesamiento (Eventos Individuales) ---"

echo "1. Esperando que Hadoop Namenode salga del modo seguro..."
until hdfs dfsadmin -safemode get | grep -q 'OFF'; do
  echo "   Namenode aún en safemode. Reintentando en 10s..."
  sleep 10
done
echo "Hadoop Namenode está listo."

echo "2. Exportando datos de MongoDB a HDFS..."
python3 /scripts_auxiliares/export_mongo_to_hdfs.py

echo "3. Ejecutando script Pig (Filtrado y Enriquecimiento)..."
pig -f /pig_scripts/01_filter_homogenize.pig

echo "4. Cargando eventos individuales a Elasticsearch..."
python3 /scripts_auxiliares/load_individual_events_to_elasticsearch.py

echo "5. Cargando eventos en caché Redis por criterios..."
python3 /scripts_auxiliares/cache_events_by_criteria.py

echo "--- Pipeline finalizado con éxito ---"
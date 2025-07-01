
import os
import sys
import subprocess
import logging
import time
import json
from elasticsearch import Elasticsearch, helpers

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "localhost")
HDFS_RESULTS_PATHS = {
    "commune_summary": "/user/hadoop/waze_analysis/commune_summary.json",
    "type_summary": "/user/hadoop/waze_analysis/type_summary.json",
    "daily_summary": "/user/hadoop/waze_analysis/daily_summary.json",
    "hourly_summary": "/user/hadoop/waze_analysis/hourly_summary.json"
}
ES_INDEX_NAME = "waze-pig-results"

def connect_to_elasticsearch():
    """Conecta a Elasticsearch con reintentos."""
    for i in range(12):
        try:
            es_client = Elasticsearch(
                [f"http://{ELASTICSEARCH_HOST}:9200"],
                max_retries=10,
                retry_on_timeout=True
            )
            if es_client.info():
                logger.info("Conexión a Elasticsearch exitosa.")
                return es_client
        except Exception as e:
            logger.warning(f"Fallo de conexión a Elasticsearch: {e}. Reintentando...")
            time.sleep(5)
    logger.error("No se pudo conectar a Elasticsearch después de varios intentos.")
    return None

def get_hdfs_data(hdfs_path):
    """Lee datos de un directorio en HDFS."""
    cat_command = ["hdfs", "dfs", "-cat", f"{hdfs_path}/part-r-*"]
    try:
        result = subprocess.run(cat_command, capture_output=True, text=True, check=False)
        if result.returncode != 0:
            logger.warning(f"No se encontraron datos en HDFS para la ruta: {hdfs_path}. Error: {result.stderr}")
            return []
        return [line for line in result.stdout.strip().split('\n') if line]
    except Exception as e:
        logger.error(f"Error inesperado al leer de HDFS '{hdfs_path}': {e}")
        return []

def create_index_if_not_exists(es_client, index_name):
    """Crea un índice en Elasticsearch si no existe."""
    if not es_client.indices.exists(index=index_name):
        try:
            es_client.indices.create(index=index_name)
            logger.info(f"Índice '{index_name}' creado.")
        except Exception as e:
            logger.error(f"Error al crear el índice '{index_name}': {e}")
            sys.exit(1)

def main():
    es_client = connect_to_elasticsearch()
    if not es_client:
        sys.exit(1)

    create_index_if_not_exists(es_client, ES_INDEX_NAME)

    actions = []
    for summary_type, hdfs_path in HDFS_RESULTS_PATHS.items():
        lines = get_hdfs_data(hdfs_path)
        if not lines:
            logger.warning(f"No se encontraron datos para '{summary_type}'.")
            continue

        logger.info(f"Procesando {len(lines)} registros para '{summary_type}'...")
        for line in lines:
            try:
                data = json.loads(line)
                action = {
                    "_index": ES_INDEX_NAME,
                    "_source": {
                        "summary_type": summary_type,
                        "data": data,
                        "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())
                    }
                }
                actions.append(action)
            except json.JSONDecodeError:
                logger.warning(f"Línea JSON mal formada en {hdfs_path}: {line}")
                continue

    if actions:
        try:
            helpers.bulk(es_client, actions)
            logger.info(f"Se han indexado {len(actions)} documentos en Elasticsearch.")
        except Exception as e:
            logger.error(f"Error al insertar datos en Elasticsearch: {e}")
    else:
        logger.warning("No se encontraron resultados de Pig para cargar en Elasticsearch.")

if __name__ == "__main__":
    main()

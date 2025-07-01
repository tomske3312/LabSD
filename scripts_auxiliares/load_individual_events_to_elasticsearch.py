#!/usr/bin/env python3
"""
Carga eventos individuales enriquecidos a Elasticsearch
"""

import json
import subprocess
import os
import logging
from datetime import datetime
from elasticsearch import Elasticsearch
from pathlib import Path

# Configuración
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

ELASTICSEARCH_HOST = os.getenv('ELASTICSEARCH_HOST', 'localhost')
HDFS_PATH = '/user/hadoop/waze_processed/individual_events'
LOCAL_TEMP_PATH = '/tmp/individual_events_data'
INDEX_NAME = 'waze-individual-events'

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
            import time
            time.sleep(5)
    logger.error("No se pudo conectar a Elasticsearch después de varios intentos.")
    return None

def download_hdfs_data():
    """Descarga datos de HDFS."""
    try:
        # Limpiar directorio temporal si existe
        import shutil
        if os.path.exists(LOCAL_TEMP_PATH):
            shutil.rmtree(LOCAL_TEMP_PATH)
            
        os.makedirs(os.path.dirname(LOCAL_TEMP_PATH), exist_ok=True)
        subprocess.run(['hdfs', 'dfs', '-get', HDFS_PATH, LOCAL_TEMP_PATH], check=True)
        logger.info(f"Datos descargados de HDFS a {LOCAL_TEMP_PATH}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error descargando de HDFS: {e}")
        return False

def parse_event_line(line):
    """Convierte línea TSV a documento Elasticsearch."""
    fields = line.strip().split('\t')
    if len(fields) < 11:
        return None
    
    try:
        # Extraer coordenadas
        lat = float(fields[4]) if fields[4] and fields[4] != 'null' else None
        lon = float(fields[5]) if fields[5] and fields[5] != 'null' else None
        
        doc = {
            'event_id': fields[0],
            'type_original': fields[1],
            'address': fields[2],
            'report_time': fields[3],
            'coordinates': {
                'lat': lat,
                'lon': lon
            } if lat and lon else None,
            'confidence': int(fields[6]) if fields[6] and fields[6] != 'null' else 0,
            'reporter': fields[7],
            'sector': fields[8] if fields[8] and fields[8] != 'null' else 'Desconocido',
            'calle': fields[9] if fields[9] and fields[9] != 'null' else '',
            'tipo_evento': fields[10],
            'hora_reporte': fields[11] if len(fields) > 11 and fields[11] != 'null' else None,
            '@timestamp': datetime.now().isoformat()
        }
        return doc
    except (ValueError, IndexError) as e:
        logger.warning(f"Error parseando línea: {e}")
        return None

def create_index_if_not_exists(es_client):
    """Crea el índice si no existe."""
    if not es_client.indices.exists(index=INDEX_NAME):
        mapping = {
            "mappings": {
                "properties": {
                    "event_id": {"type": "keyword"},
                    "type_original": {"type": "keyword"},
                    "address": {"type": "text"},
                    "report_time": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||epoch_millis"},
                    "coordinates": {"type": "geo_point"},
                    "confidence": {"type": "integer"},
                    "reporter": {"type": "keyword"},
                    "sector": {"type": "keyword"},
                    "calle": {"type": "text"},
                    "tipo_evento": {"type": "keyword"},
                    "hora_reporte": {"type": "keyword"},
                    "@timestamp": {"type": "date"}
                }
            }
        }
        es_client.indices.create(index=INDEX_NAME, body=mapping)
        logger.info(f"Índice '{INDEX_NAME}' creado con mapping optimizado.")

def load_events_to_elasticsearch(es_client):
    """Carga eventos individuales a Elasticsearch."""
    if not download_hdfs_data():
        return False
    
    create_index_if_not_exists(es_client)
    
    # Leer eventos y prepararlos para bulk insert
    documents = []
    hdfs_dir = Path(LOCAL_TEMP_PATH)
    
    for part_file in hdfs_dir.glob('part-*'):
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                for line in f:
                    doc = parse_event_line(line)
                    if doc:
                        documents.append({
                            "_index": INDEX_NAME,
                            "_source": doc
                        })
        except Exception as e:
            logger.error(f"Error leyendo archivo {part_file}: {e}")
    
    # Bulk insert
    if documents:
        from elasticsearch.helpers import bulk
        try:
            bulk(es_client, documents)
            logger.info(f"Se han indexado {len(documents)} eventos individuales en Elasticsearch.")
            return True
        except Exception as e:
            logger.error(f"Error en bulk insert: {e}")
            return False
    else:
        logger.warning("No se encontraron documentos para indexar.")
        return False

def main():
    es_client = connect_to_elasticsearch()
    if not es_client:
        return False
    
    return load_events_to_elasticsearch(es_client)

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
Script para exportar datos de MongoDB a Elasticsearch para visualización en Kibana.
Parte del pipeline de análisis de tráfico.
"""

import os
import sys
import time
import logging
import json
from datetime import datetime
import pymongo
from pymongo.errors import ConnectionFailure
from elasticsearch import Elasticsearch
from elasticsearch.exceptions import ConnectionError as ESConnectionError, RequestError

# Configuración de logging
logging.basicConfig(
    level=logging.INFO, 
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger(__name__)

# Variables de entorno
MONGO_HOST = os.environ.get("MONGO_HOST", "storage_db")
ELASTICSEARCH_HOST = os.environ.get("ELASTICSEARCH_HOST", "elasticsearch")
ES_INDEX_NAME = "waze-events"

def connect_to_mongodb():
    """Conectar a MongoDB con reintentos"""
    for attempt in range(10):
        try:
            client = pymongo.MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logger.info(f"Conexión exitosa a MongoDB en {MONGO_HOST}")
            return client
        except ConnectionFailure as err:
            logger.warning(f"Intento {attempt + 1}: Error conectando a MongoDB: {err}")
            time.sleep(10)
    
    logger.error("No se pudo conectar a MongoDB después de múltiples intentos")
    return None

def connect_to_elasticsearch():
    """Conectar a Elasticsearch con reintentos"""
    for attempt in range(20):
        try:
            es = Elasticsearch([f"http://{ELASTICSEARCH_HOST}:9200"])
            if es.ping():
                logger.info(f"Conexión exitosa a Elasticsearch en {ELASTICSEARCH_HOST}")
                return es
            else:
                raise ESConnectionError("Ping falló")
        except ESConnectionError as err:
            logger.warning(f"Intento {attempt + 1}: Error conectando a Elasticsearch: {err}")
            time.sleep(15)
    
    logger.error("No se pudo conectar a Elasticsearch después de múltiples intentos")
    return None

def create_index_mapping(es):
    """Crear el índice con el mapping apropiado"""
    mapping = {
        "mappings": {
            "properties": {
                "event_id": {"type": "keyword"},
                "type": {"type": "text", "analyzer": "standard"},
                "address": {"type": "text", "analyzer": "standard"},
                "city": {"type": "keyword"},
                "scrape_timestamp": {"type": "date", "format": "yyyy-MM-dd HH:mm:ss||yyyy-MM-dd'T'HH:mm:ss"},
                "location": {"type": "geo_point"},
                "commune": {"type": "keyword"},
                "standardized_type": {"type": "keyword"},
                "ingestion_timestamp": {"type": "date"}
            }
        },
        "settings": {
            "number_of_shards": 1,
            "number_of_replicas": 0
        }
    }
    
    try:
        if es.indices.exists(index=ES_INDEX_NAME):
            logger.info(f"Índice {ES_INDEX_NAME} ya existe")
        else:
            es.indices.create(index=ES_INDEX_NAME, body=mapping)
            logger.info(f"Índice {ES_INDEX_NAME} creado con mapping")
    except RequestError as e:
        if "resource_already_exists_exception" not in str(e):
            logger.error(f"Error creando índice: {e}")
            return False
    return True

def process_and_index_events(mongo_client, es):
    """Procesar eventos de MongoDB e indexarlos en Elasticsearch"""
    collection = mongo_client.waze_data.events
    
    # Obtener eventos desde MongoDB
    events = list(collection.find({}))
    if not events:
        logger.warning("No se encontraron eventos en MongoDB")
        return 0
    
    logger.info(f"Procesando {len(events)} eventos de MongoDB")
    
    indexed_count = 0
    for event in events:
        try:
            # Preparar el documento para Elasticsearch
            doc = {
                "event_id": event.get("event_id", ""),
                "type": event.get("type", ""),
                "address": event.get("address", ""),
                "city": event.get("city", ""),
                "scrape_timestamp": event.get("scrape_timestamp", ""),
                "ingestion_timestamp": datetime.now().isoformat()
            }
            
            # Añadir location si hay coordenadas
            if "lat" in event and "lon" in event:
                try:
                    doc["location"] = {
                        "lat": float(event["lat"]),
                        "lon": float(event["lon"])
                    }
                except (ValueError, TypeError):
                    pass
            
            # Enriquecer con campo comuna (similar a la lógica de Pig)
            address = doc["address"].lower()
            city = doc["city"].lower()
            
            if "las condes" in address or "las condes" in city:
                doc["commune"] = "Las Condes"
            elif "santiago" in address or "santiago" in city:
                doc["commune"] = "Santiago"
            elif "providencia" in address or "providencia" in city:
                doc["commune"] = "Providencia"
            else:
                doc["commune"] = "Otra Comuna"
            
            # Estandarizar tipo de evento
            event_type = doc["type"].lower()
            if any(word in event_type for word in ["accident", "crash"]):
                doc["standardized_type"] = "Accidente"
            elif any(word in event_type for word in ["jam", "traffic"]):
                doc["standardized_type"] = "Atasco"
            elif any(word in event_type for word in ["closed", "closure"]):
                doc["standardized_type"] = "Corte de Ruta"
            elif any(word in event_type for word in ["hazard", "police"]):
                doc["standardized_type"] = "Peligro en Via"
            else:
                doc["standardized_type"] = "Otro"
            
            # Indexar en Elasticsearch
            es.index(
                index=ES_INDEX_NAME,
                id=doc["event_id"],
                body=doc
            )
            indexed_count += 1
            
        except Exception as e:
            logger.error(f"Error procesando evento {event.get('event_id', 'unknown')}: {e}")
            continue
    
    logger.info(f"Indexados {indexed_count} eventos en Elasticsearch")
    return indexed_count

def main():
    """Función principal"""
    logger.info("=== Iniciando exportación MongoDB → Elasticsearch ===")
    
    # Conectar a MongoDB
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        sys.exit(1)
    
    # Conectar a Elasticsearch
    es = connect_to_elasticsearch()
    if not es:
        mongo_client.close()
        sys.exit(1)
    
    # Crear índice con mapping
    if not create_index_mapping(es):
        mongo_client.close()
        sys.exit(1)
    
    # Procesar e indexar eventos
    indexed_count = process_and_index_events(mongo_client, es)
    
    # Cerrar conexiones
    mongo_client.close()
    
    if indexed_count > 0:
        logger.info(f"=== Exportación completada: {indexed_count} eventos indexados ===")
        logger.info(f"Los datos están disponibles en Kibana en el índice '{ES_INDEX_NAME}'")
    else:
        logger.warning("=== No se indexaron eventos ===")

if __name__ == "__main__":
    main()

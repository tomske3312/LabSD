# importer/import_to_mongo.py
import json
import os
import sys
import time
import logging
import shutil
from datetime import datetime
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure
import pymongo.errors

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

MONGO_HOST = os.getenv('MONGO_HOST', 'storage_db')
JSON_FILE_PATH = '/app/data/waze_events.json'
PROCESSED_FILE_DIR = os.path.join(os.path.dirname(JSON_FILE_PATH), "processed_events")
CHECK_INTERVAL_SECONDS = 15

os.makedirs(PROCESSED_FILE_DIR, exist_ok=True)

def connect_to_mongodb():
    for attempt in range(12):
        try:
            client = MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            logger.info("Conexión a MongoDB exitosa.")
            return client
        except ConnectionFailure as e:
            logger.warning(f"Fallo de conexión a MongoDB: {e}. Reintentando...")
            time.sleep(CHECK_INTERVAL_SECONDS)
    return None

def ensure_mongo_index(collection):
    try:
        collection.create_index([("event_id", pymongo.ASCENDING)], unique=True)
    except OperationFailure:
        logger.info("El índice 'event_id' ya existe.")

def import_events(collection, events):
    if not events: return 0, 0
    operations = [pymongo.UpdateOne({'event_id': e.get('event_id')}, {'$set': e}, upsert=True) for e in events if e.get('event_id')]
    if not operations: return 0, 0
    try:
        result = collection.bulk_write(operations, ordered=False)
        return result.upserted_count, result.modified_count
    except pymongo.errors.BulkWriteError as bwe:
        return bwe.details.get('nUpserted', 0), bwe.details.get('nModified', 0)
    return 0, 0

def process_file(collection):
    if not os.path.exists(JSON_FILE_PATH): return
    logger.info(f"Archivo JSON encontrado. Procesando...")
    try:
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            events = json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        os.rename(JSON_FILE_PATH, f"{JSON_FILE_PATH}.corrupt_{int(time.time())}")
        return
    
    if events:
        imported, updated = import_events(collection, events)
        logger.info(f"Resultado -> Nuevos: {imported}, Actualizados: {updated}.")
    
    # CAMBIO: Copiar en lugar de mover para preservar el archivo original
    processed_path = os.path.join(PROCESSED_FILE_DIR, f"waze_events.{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
    shutil.copy2(JSON_FILE_PATH, processed_path)
    logger.info(f"Archivo copiado como backup en {processed_path}.")

def main():
    mongo_client = connect_to_mongodb()
    if not mongo_client: sys.exit(1)
    db = mongo_client.waze_data
    collection = db.events
    ensure_mongo_index(collection)
    try:
        while True:
            process_file(collection)
            logger.info(f"Total de eventos en DB: {collection.count_documents({})}. Esperando...")
            time.sleep(CHECK_INTERVAL_SECONDS)
    except KeyboardInterrupt:
        logger.info("Interrupción manual.")
    finally:
        mongo_client.close()

if __name__ == '__main__':
    main()
# -*- coding: utf-8 -*-

import json
import sys
import logging
import os # Para verificar si existe el JSON

# --- Dependencias ---
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, BulkWriteError
except ModuleNotFoundError:
    print("ERROR: El módulo 'pymongo' no está instalado. Ejecuta: pip install pymongo")
    sys.exit(1)
# Ya no se necesitan pytz ni dateutil

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger()

# --- Configuración ---
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo_db')
MONGO_URI = f"mongodb://{MONGO_HOST}:27017/"
DATABASE_NAME = "waze_data"
COLLECTION_NAME = "events"
JSON_FILE_PATH = os.getenv('JSON_FILE', '/app/data/waze_events.json')

# Ya no necesitamos parse_utc_timestamp ni adjust_timestamp

def import_data():
    """Lee el archivo JSON e inserta/actualiza los datos en MongoDB usando event_id."""
    logger.info(f"Intentando conectar a MongoDB en: {MONGO_URI}")
    try:
        client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
        client.admin.command('ping')
        logger.info("Conexión a MongoDB exitosa.")
    except ConnectionFailure as e:
        logger.error(f"Error al conectar a MongoDB: {e}")
        sys.exit(1)

    db = client[DATABASE_NAME]
    collection = db[COLLECTION_NAME]
    logger.info(f"Usando base de datos '{DATABASE_NAME}' y colección '{COLLECTION_NAME}'.")

    try:
        logger.info(f"Leyendo datos desde {JSON_FILE_PATH}...")
        if not os.path.exists(JSON_FILE_PATH):
             logger.error(f"Error: El archivo {JSON_FILE_PATH} no existe.")
             sys.exit(1)
        with open(JSON_FILE_PATH, 'r', encoding='utf-8') as f:
            try: events_data = json.load(f)
            except json.JSONDecodeError as e:
                logger.error(f"Error al decodificar JSON en {JSON_FILE_PATH}: {e}")
                sys.exit(1)

        if not isinstance(events_data, list):
            logger.error(f"Error: El archivo JSON no contiene una lista de eventos.")
            sys.exit(1)
        if not events_data:
            logger.warning(f"El archivo {JSON_FILE_PATH} está vacío.")
            return

        logger.info(f"Se encontraron {len(events_data)} eventos en el archivo JSON.")
        upserted_count = 0; modified_count = 0; skipped_count = 0

        for event in events_data:
            # --- CAMBIO: Usar 'event_id' del JSON como filtro ---
            event_id_from_scraper = event.get('event_id')

            if not event_id_from_scraper:
                logger.warning(f"Evento omitido por falta de 'event_id': {event}")
                skipped_count += 1; continue

            # Filtro para buscar/actualizar basado en el event_id del scraper
            upsert_filter = { "event_id": event_id_from_scraper }

            # Datos a insertar/actualizar (el evento completo del JSON)
            # Ya no necesitamos calcular ni ajustar timestamps aquí
            event_data_for_mongo = event.copy()
            # Eliminar campos calculados si existieran por error en el JSON
            event_data_for_mongo.pop('timestamp_calculated_utc', None)
            event_data_for_mongo.pop('timestamp_local_adjusted', None)


            try:
                result = collection.update_one(
                    upsert_filter,
                    {"$set": event_data_for_mongo}, # Actualiza todo el documento si ya existe
                    upsert=True # Inserta si no existe
                )
                if result.upserted_id: upserted_count += 1
                elif result.modified_count > 0: modified_count += 1

            except Exception as e:
                logger.error(f"Error haciendo upsert para el evento con ID '{event_id_from_scraper}': {e}")
                skipped_count += 1

        logger.info("Proceso de importación/actualización completado.")
        logger.info(f"Resultados: Insertados={upserted_count}, Actualizados={modified_count}, Omitidos={skipped_count}")

    except FileNotFoundError: logger.error(f"Error: No se encontró el archivo {JSON_FILE_PATH}.")
    except Exception as e: logger.exception(f"Ocurrió un error inesperado durante la importación: {e}")
    finally:
        logger.info("Cerrando conexión a MongoDB.")
        client.close()

if __name__ == "__main__":
    import_data()
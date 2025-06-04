import json
import os
import time
import pymongo
from pymongo import MongoClient
from pymongo.errors import ConnectionFailure, OperationFailure, ServerSelectionTimeoutError
from datetime import datetime
import logging

# --- Configuración de Logging ---
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

MONGO_HOST = os.getenv('MONGO_HOST', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB_NAME = os.getenv('MONGO_DB_NAME', 'waze_data')
MONGO_COLLECTION_NAME = os.getenv('MONGO_COLLECTION_NAME', 'events')
JSON_FILE = os.getenv('JSON_FILE', '/app/data/waze_events.json')
PROCESSED_FILE_DIR = os.path.join(os.path.dirname(JSON_FILE), "processed_events")

# Crear el directorio para archivos procesados si no existe
os.makedirs(PROCESSED_FILE_DIR, exist_ok=True)


# Parámetros para la espera inicial y reintentos
MIN_EVENTS_THRESHOLD = 500  # Un objetivo mínimo de eventos para empezar el procesamiento "real"
MAX_WAIT_ATTEMPTS = 120    # Máximo de intentos para esperar el archivo JSON o los eventos (120 * 5s = 10 minutos)
WAIT_BETWEEN_ATTEMPTS = 5  # Segundos de espera entre intentos

def connect_to_mongodb():
    """Intenta conectar a MongoDB y devuelve el cliente."""
    client = None
    for i in range(MAX_WAIT_ATTEMPTS):
        try:
            client = MongoClient(f"mongodb://{MONGO_HOST}:{MONGO_PORT}/", serverSelectionTimeoutMS=5000)
            client.admin.command('ping') # Comando ligero para verificar conexión
            logger.info(f"Conexión a MongoDB exitosa en {MONGO_HOST}:{MONGO_PORT}")
            return client
        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.warning(f"Fallo de conexión a MongoDB (Intento {i+1}/{MAX_WAIT_ATTEMPTS}): {e}. Reintentando en {WAIT_BETWEEN_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_ATTEMPTS)
        except Exception as e:
            logger.error(f"Error inesperado al conectar a MongoDB (Intento {i+1}/{MAX_WAIT_ATTEMPTS}): {e}. Reintentando en {WAIT_BETWEEN_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_ATTEMPTS)
    logger.error("No se pudo conectar a MongoDB después de varios intentos. Terminando.")
    return None

def import_events_to_mongo(collection, events_to_import):
    """Importa una lista de eventos a MongoDB, manejando duplicados con upsert."""
    imported_count = 0
    updated_count = 0
    # Usar un bulk write para eficiencia
    operations = []

    for event_data in events_to_import:
        # Usar el 'event_id' que ahora incluye el timestamp de scrape como un identificador único.
        # Si quieres que Mongo trate eventos similares como el mismo (ej. mismo tipo, dirección, reportero)
        # aunque el scraper los haya recogido en diferentes momentos y les haya dado diferentes `event_id`s,
        # necesitarías generar una clave `_id` o de `query` basada en los campos "lógicos" del evento
        # (tipo, dirección, reportero), y luego usar `upsert` con esa clave.
        # Para este proyecto, el `event_id` del scraper se asume como único para el upsert.
        
        event_id = event_data.get('event_id')
        if not event_id:
            logger.warning(f"Evento sin 'event_id', saltando: {event_data}")
            continue

        operations.append(
            pymongo.UpdateOne(
                {'event_id': event_id},
                {'$set': event_data},
                upsert=True
            )
        )
    
    if not operations:
        return 0, 0 # No hay operaciones para ejecutar

    try:
        result = collection.bulk_write(operations, ordered=False) # ordered=False para continuar si hay un error en una operación
        imported_count = result.upserted_count
        updated_count = result.modified_count
    except OperationFailure as e:
        logger.error(f"Error de MongoDB durante bulk write: {e}")
        # Puedes analizar e.details para errores más específicos si necesitas.
    except Exception as e:
        logger.error(f"Error inesperado durante bulk write a MongoDB: {e}")
            
    return imported_count, updated_count


def load_and_process_json_file(collection):
    """Carga eventos del archivo JSON, los importa a MongoDB y renombra el archivo (para evitar reprocesar)."""
    if not os.path.exists(JSON_FILE):
        return 0, 0, collection.count_documents({}) # newly_imported, newly_updated, total_in_mongo

    events_from_file = []
    # Añadir reintentos para leer el archivo JSON, ya que el scraper puede estar escribiendo en él
    max_json_read_attempts = 5
    for attempt in range(max_json_read_attempts):
        try:
            with open(JSON_FILE, 'r', encoding='utf-8') as f:
                events_from_file = json.load(f)
            break # Si la lectura es exitosa, salir del bucle de reintentos
        except json.JSONDecodeError as e:
            logger.warning(f"Error al decodificar JSON del archivo {JSON_FILE} (Intento {attempt+1}/{max_json_read_attempts}): {e}. Podría estar incompleto. Reintentando en 1s...")
            time.sleep(1)
        except Exception as e:
            logger.warning(f"Error al leer archivo {JSON_FILE} (Intento {attempt+1}/{max_json_read_attempts}): {e}. Reintentando en 1s...")
            time.sleep(1)
    else: # Si el bucle de reintentos se agota sin éxito
        logger.error(f"Fallo persistente al leer el archivo JSON {JSON_FILE} después de {max_json_read_attempts} intentos. El archivo puede estar corrupto o bloqueado. Renombrando para evitar más problemas.")
        timestamp_err = datetime.now().strftime("%Y%m%d%H%M%S")
        try:
            os.rename(JSON_FILE, JSON_FILE + f".corrupt_{timestamp_err}")
        except OSError as oe:
            logger.error(f"No se pudo renombrar el archivo corrupto {JSON_FILE}: {oe}. Puede que ya no exista.")
        return 0, 0, collection.count_documents({})


    if not events_from_file:
        logger.info(f"El archivo {JSON_FILE} está vacío.")
        # Renombrar archivos vacíos para que no se reprocesen.
        timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
        processed_file_path = os.path.join(PROCESSED_FILE_DIR, f"{os.path.basename(JSON_FILE)}.empty_{timestamp}")
        try:
            os.rename(JSON_FILE, processed_file_path)
            logger.info(f"Archivo vacío {JSON_FILE} renombrado a {processed_file_path}.")
        except OSError as oe:
            logger.error(f"No se pudo renombrar el archivo vacío {JSON_FILE}: {oe}. Puede que ya no exista.")
        return 0, 0, collection.count_documents({})


    logger.info(f"Cargados {len(events_from_file)} eventos del archivo {JSON_FILE}.")
    imported_new, updated_existing = import_events_to_mongo(collection, events_from_file)
    
    # Renombrar el archivo después de procesarlo para que el scraper pueda crear uno nuevo
    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    processed_file_path = os.path.join(PROCESSED_FILE_DIR, f"{os.path.basename(JSON_FILE)}.processed_{timestamp}")
    try:
        os.rename(JSON_FILE, processed_file_path)
        logger.info(f"Archivo {JSON_FILE} procesado y renombrado a {processed_file_path}.")
    except OSError as e:
        logger.error(f"Error al renombrar el archivo {JSON_FILE} a {processed_file_path}: {e}. Esto puede causar reprocesamiento.")
    
    total_events_in_mongo = collection.count_documents({})
    return imported_new, updated_existing, total_events_in_mongo

def main():
    logger.info("--- Iniciando Script Importador a MongoDB ---")
    
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        return

    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    # Bucle inicial para esperar la primera carga significativa de eventos del scraper
    total_events_in_mongo = 0
    wait_attempts = 0
    while total_events_in_mongo < MIN_EVENTS_THRESHOLD and wait_attempts < MAX_WAIT_ATTEMPTS:
        current_total_before_check = collection.count_documents({})
        logger.info(f"Intento {wait_attempts+1}/{MAX_WAIT_ATTEMPTS} de espera inicial: {current_total_before_check} eventos en MongoDB. Esperando {MIN_EVENTS_THRESHOLD}...")
        
        newly_imported, newly_updated, current_total_after_check = load_and_process_json_file(collection)
        total_events_in_mongo = current_total_after_check
        
        if newly_imported > 0 or newly_updated > 0: # Si se importaron eventos, notificar
            logger.info(f"Eventos importados en esta ronda: {newly_imported} (nuevos), {newly_updated} (actualizados). Total en Mongo: {total_events_in_mongo}.")
        
        if total_events_in_mongo >= MIN_EVENTS_THRESHOLD:
            logger.info(f"¡Umbral de {MIN_EVENTS_THRESHOLD} eventos alcanzado! Continuando con el procesamiento principal.")
            break
        
        time.sleep(WAIT_BETWEEN_ATTEMPTS)
        wait_attempts += 1
    
    if total_events_in_mongo < MIN_EVENTS_THRESHOLD:
        logger.warning(f"Advertencia: No se alcanzó el umbral de {MIN_EVENTS_THRESHOLD} eventos después de {MAX_WAIT_ATTEMPTS} intentos. Iniciando el ciclo principal con {total_events_in_mongo} eventos.")
    else:
        logger.info(f"Carga inicial de eventos completada. Total: {total_events_in_mongo}")

    # Bucle principal para procesamiento continuo de nuevos archivos JSON
    while True:
        try:
            imported_this_round, updated_this_round, total_in_mongo = load_and_process_json_file(collection)
            if imported_this_round > 0 or updated_this_round > 0:
                logger.info(f"Total de eventos en MongoDB: {total_in_mongo} (nuevos en esta ronda: {imported_this_round}, actualizados: {updated_this_round})")
            else:
                logger.debug(f"No se encontraron nuevos archivos para importar en esta ronda. Total en Mongo: {total_in_mongo}")
            
            # Pausa entre rondas de importación para no sobrecargar el sistema
            time.sleep(10) # Puedes ajustar esta pausa

        except KeyboardInterrupt:
            logger.info("Interrupción manual. Deteniendo importador.")
            break
        except Exception as e:
            logger.error(f"Error inesperado en el bucle principal del importador: {e}")
            time.sleep(15) # Pausa más larga en caso de error para evitar loops rápidos

    mongo_client.close()
    logger.info("--- Script Importador a MongoDB finalizado ---")

if __name__ == '__main__':
    main()

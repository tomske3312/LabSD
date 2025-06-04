import pymongo
import json
import os
import subprocess
import logging
import time # Añadir import para time.sleep
from datetime import datetime

# Configuración de Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

MONGO_HOST = os.environ.get("MONGO_HOST", "mongo_db") # Nombre del servicio MongoDB
MONGO_PORT = 27017
MONGO_DB_NAME = "waze_data"
MONGO_COLLECTION_NAME = "events"

# Ruta temporal DENTRO del contenedor pig-runner donde se guardará el TSV
LOCAL_TSV_PATH = "/tmp/waze_events_for_hdfs.tsv"
# HDFS_BASE_PATH debe ser un directorio donde Pig pueda leer. Usaremos /user/hadoop/
HDFS_INPUT_DIR = "/user/hadoop/waze_input"
HDFS_TARGET_PATH = os.path.join(HDFS_INPUT_DIR, "waze_events.tsv") # Nombre del archivo dentro de HDFS

# Parámetros de reintento para la conexión a MongoDB
MAX_MONGO_CONNECT_ATTEMPTS = 60 # 60 intentos * 5s = 5 minutos
WAIT_BETWEEN_MONGO_ATTEMPTS = 5 # segundos

def connect_to_mongodb():
    client = None
    for i in range(MAX_MONGO_CONNECT_ATTEMPTS):
        try:
            logger.info(f"Intentando conectar a MongoDB en: mongodb://{MONGO_HOST}:{MONGO_PORT}/ (Intento {i+1}/{MAX_MONGO_CONNECT_ATTEMPTS})")
            client = pymongo.MongoClient(MONGO_HOST, MONGO_PORT, serverSelectionTimeoutMS=5000)
            client.admin.command('ping') # Verificar conexión
            logger.info("Conexión a MongoDB exitosa.")
            return client
        except pymongo.errors.ServerSelectionTimeoutError as err:
            logger.warning(f"Fallo de conexión a MongoDB (Timeout): {err}. Reintentando en {WAIT_BETWEEN_MONGO_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_MONGO_ATTEMPTS)
        except Exception as e:
            logger.warning(f"Error inesperado al conectar a MongoDB: {e}. Reintentando en {WAIT_BETWEEN_MONGO_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_MONGO_ATTEMPTS)
    logger.error("No se pudo conectar a MongoDB después de varios intentos. Terminando.")
    return None


def get_mongo_data(collection):
    """Extrae todos los documentos de la colección MongoDB."""
    try:
        logger.info(f"Extrayendo datos de la colección '{MONGO_COLLECTION_NAME}'...")
        # Obtener un conteo de documentos antes de extraer para loggear
        doc_count = collection.estimated_document_count()
        logger.info(f"Se encontraron {doc_count} documentos en MongoDB. Extrayendo...")
        
        documents = list(collection.find({}))
        logger.info(f"Se extrajeron {len(documents)} documentos de MongoDB.")
        return documents
    except Exception as e:
        logger.exception(f"Error al obtener datos de MongoDB: {e}")
        return []

def prepare_data_for_pig(documents):
    """
    Prepara los datos para Pig en formato TSV.
    Selecciona los campos relevantes y maneja valores faltantes.
    Asegúrate de que estos campos coincidan con lo que el scraper captura.
    """
    pig_data = []
    # Definir explícitamente los campos esperados y su orden
    # Basado en scrape_waze.py, no hay latitude/longitude directamente
    expected_fields = [
        "event_id", "type", "address",
        "timestamp_original_relative", "reporter", "scrape_timestamp"
    ]

    for doc in documents:
        row = []
        for field in expected_fields:
            value = doc.get(field)
            if value is None:
                value = "" # Reemplazar None con cadena vacía para TSV
            # Limpiar tabs, newlines, retornos de carro para no romper el formato TSV
            value_str = str(value).replace('\t', ' ').replace('\n', ' ').replace('\r', ' ')
            row.append(value_str)
        pig_data.append("\t".join(row))
    return pig_data

def save_to_local_tsv(pig_data_lines):
    """
    Guarda las líneas de datos en un archivo TSV local de forma segura,
    usando un archivo temporal para atomicidad.
    """
    try:
        logger.info(f"Guardando datos en archivo TSV local: {LOCAL_TSV_PATH}")
        temp_tsv_path = LOCAL_TSV_PATH + ".tmp"
        with open(temp_tsv_path, "w", encoding="utf-8") as f:
            for line in pig_data_lines:
                f.write(line + "\n")
        os.rename(temp_tsv_path, LOCAL_TSV_PATH) # Renombrar para hacer la escritura atómica
        logger.info(f"Datos guardados localmente en {LOCAL_TSV_PATH}. {len(pig_data_lines)} líneas.")
        return True
    except Exception as e:
        logger.exception(f"Error al guardar el archivo TSV local: {e}")
        return False

def run_hdfs_command(command, check_output=False, allow_failure=False):
    """
    Ejecuta un comando HDFS de forma segura.
    Args:
        command (list): Lista de strings representando el comando y sus argumentos.
        check_output (bool): Si True, captura y devuelve la salida estándar.
        allow_failure (bool): Si True, no lanza una excepción si el comando falla.
    Returns:
        tuple: (True/False, salida_stdout, salida_stderr)
    """
    cmd_str = ' '.join(command)
    try:
        logger.info(f"Ejecutando comando HDFS: {cmd_str}")
        result = subprocess.run(command, capture_output=True, text=True, check=(not allow_failure))
        
        if result.returncode != 0 and not allow_failure:
            logger.error(f"Comando HDFS falló con código {result.returncode}: {cmd_str}")
            logger.error(f"Stdout: {result.stdout.strip()}")
            logger.error(f"Stderr: {result.stderr.strip()}")
            return False, result.stdout, result.stderr
        elif result.returncode != 0 and allow_failure:
            logger.warning(f"Comando HDFS falló (permitido): {cmd_str} - Stderr: {result.stderr.strip()}")
            return False, result.stdout, result.stderr

        logger.debug(f"Comando HDFS exitoso: {cmd_str}")
        return True, result.stdout, result.stderr

    except FileNotFoundError:
        logger.error(f"Comando HDFS '{command[0]}' no encontrado. Asegúrate que Hadoop esté en el PATH.")
        return False, "", "Command not found"
    except subprocess.CalledProcessError as e: # Esto solo ocurriría si check=True
        logger.error(f"Error en subprocess.CalledProcessError: {e}")
        return False, e.stdout, e.stderr
    except Exception as e:
        logger.exception(f"Error inesperado al ejecutar comando HDFS: {e}")
        return False, "", str(e)


def upload_to_hdfs():
    """
    Sube el archivo TSV local a HDFS, asegurando que el directorio exista
    y eliminando el archivo anterior si es necesario.
    """
    logger.info(f"Iniciando subida a HDFS para {HDFS_TARGET_PATH}")

    # 1. Asegurar que el directorio de destino en HDFS exista
    success, _, stderr = run_hdfs_command(["hdfs", "dfs", "-mkdir", "-p", HDFS_INPUT_DIR])
    if not success:
        logger.error(f"No se pudo crear o asegurar el directorio HDFS '{HDFS_INPUT_DIR}': {stderr}")
        return False

    # 2. Eliminar el archivo existente en HDFS (si existe)
    # Usar allow_failure=True porque fallará si el archivo no existe, lo cual es normal.
    run_hdfs_command(["hdfs", "dfs", "-rm", "-f", "-skipTrash", HDFS_TARGET_PATH], allow_failure=True)
    logger.info(f"Archivo existente {HDFS_TARGET_PATH} eliminado o no existía.")

    # 3. Subir el nuevo archivo a HDFS
    success, _, stderr = run_hdfs_command(["hdfs", "dfs", "-put", LOCAL_TSV_PATH, HDFS_TARGET_PATH])
    if success:
        logger.info(f"Archivo subido exitosamente a HDFS en {HDFS_TARGET_PATH}")
        return True
    else:
        logger.error(f"Fallo al subir el archivo local '{LOCAL_TSV_PATH}' a HDFS.")
        return False

if __name__ == "__main__":
    logger.info("--- Iniciando script de exportación de MongoDB a HDFS ---")
    
    mongo_client = connect_to_mongodb()
    if not mongo_client:
        logger.error("No se pudo conectar a MongoDB. Abortando exportación a HDFS.")
        sys.exit(1) # Salir con error

    db = mongo_client[MONGO_DB_NAME]
    collection = db[MONGO_COLLECTION_NAME]

    mongo_documents = get_mongo_data(collection)
    
    # Cierre explícito del cliente Mongo después de obtener los datos
    mongo_client.close()
    logger.info("Conexión a MongoDB cerrada.")

    if mongo_documents:
        pig_formatted_lines = prepare_data_for_pig(mongo_documents)
        if pig_formatted_lines:
            if save_to_local_tsv(pig_formatted_lines):
                if upload_to_hdfs():
                    logger.info("Proceso de exportación a HDFS completado exitosamente.")
                else:
                    logger.error("Fallo al subir el archivo TSV a HDFS.")
                    sys.exit(1)
            else:
                logger.error("Fallo al guardar el archivo TSV local.")
                sys.exit(1)
        else:
            logger.warning("No hay datos formateados para Pig para guardar.")
            sys.exit(0) # Salir sin error si no hay datos para procesar
    else:
        logger.warning("No se obtuvieron documentos de MongoDB para exportar.")
        sys.exit(0) # Salir sin error si no hay datos
    logger.info("--- Script de exportación de MongoDB a HDFS finalizado ---")

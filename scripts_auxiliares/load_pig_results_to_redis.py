import redis
import subprocess
import os
import logging
import time # Añadir import para time.sleep
import json # Importar json para parsear los resultados de Pig

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "redis_cache") # Nombre del servicio Redis
REDIS_PORT = 6379

# Rutas de resultados en HDFS (deben coincidir con 02_analyze_data.pig)
HDFS_RESULTS_BASE_DIR = "/user/hadoop/waze_analysis" # Usar /user/hadoop/
HDFS_RESULTS_PATHS = {
    "commune_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "commune_summary.json"),
    "type_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "type_summary.json"),
    "daily_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "daily_summary.json"),
    "hourly_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "hourly_summary.json")
}

# Parámetros de reintento para la conexión a Redis
MAX_REDIS_CONNECT_ATTEMPTS = 30 # 30 intentos * 2s = 1 minuto
WAIT_BETWEEN_REDIS_ATTEMPTS = 2 # segundos

def connect_to_redis():
    r_client = None
    for i in range(MAX_REDIS_CONNECT_ATTEMPTS):
        try:
            logger.info(f"Intentando conectar a Redis en: {REDIS_HOST}:{REDIS_PORT} (Intento {i+1}/{MAX_REDIS_CONNECT_ATTEMPTS})")
            r_client = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
            r_client.ping()
            logger.info("Conexión a Redis exitosa.")
            return r_client
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Fallo de conexión a Redis: {e}. Reintentando en {WAIT_BETWEEN_REDIS_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_REDIS_ATTEMPTS)
        except Exception as e:
            logger.warning(f"Error inesperado al conectar a Redis: {e}. Reintentando en {WAIT_BETWEEN_REDIS_ATTEMPTS}s...")
            time.sleep(WAIT_BETWEEN_REDIS_ATTEMPTS)
    logger.error("No se pudo conectar a Redis después de varios intentos. Terminando.")
    return None

def get_hdfs_data(hdfs_path):
    """
    Lee todos los archivos part-* de un directorio HDFS (o un archivo específico)
    y devuelve las líneas. Maneja casos donde no hay archivos.
    """
    target_path = hdfs_path # Puede ser un directorio o un archivo
    
    # Primero, verificar si el path existe y si es un directorio
    # Si es un directorio, intentamos listar sus contenidos para encontrar part-*
    # Si es un archivo, -cat lo leerá directamente.
    
    # Intenta listar para ver si es un directorio con archivos part-*
    list_command = ["hdfs", "dfs", "-ls", target_path]
    process_list = subprocess.run(list_command, capture_output=True, text=True, check=False)

    if process_list.returncode != 0:
        logger.warning(f"El directorio o archivo HDFS '{target_path}' no existe o está inaccesible. Stderr: {process_list.stderr.strip()}")
        return []
    
    # Si la lista contiene "part-", asume que hay múltiples archivos
    if "part-" in process_list.stdout:
        cat_command = ["hdfs", "dfs", "-cat", f"{target_path}/part-*"]
    else: # Si no, asume que es un único archivo o un wildcard ya definido
        cat_command = ["hdfs", "dfs", "-cat", target_path]

    try:
        logger.info(f"Ejecutando: {' '.join(cat_command)}")
        process = subprocess.run(cat_command, capture_output=True, text=True, check=False)
        
        if process.returncode != 0:
            logger.error(f"Error al leer de HDFS '{target_path}'. Código de salida: {process.returncode}")
            logger.error(f"Stderr: {process.stderr.strip()}")
            return []

        if not process.stdout.strip():
            logger.warning(f"No se encontraron datos en HDFS para '{target_path}'.")
            return []

        lines = process.stdout.strip().split('\n')
        logger.info(f"Leídas {len(lines)} líneas desde {target_path}")
        return lines
    except FileNotFoundError:
        logger.error("Comando 'hdfs' no encontrado. Asegúrate que Hadoop esté en el PATH.")
        return []
    except subprocess.CalledProcessError as e:
        logger.error(f"Error al ejecutar comando HDFS para leer '{target_path}': {e}")
        logger.error(f"Salida: {e.output.strip()}")
        logger.error(f"Error: {e.stderr.strip()}")
        return []
    except Exception as e:
        logger.exception(f"Error inesperado al leer datos de HDFS '{target_path}': {e}")
        return []

def load_to_redis(r_client):
    success_count = 0

    # 1. Cargar 'commune_summary' a Redis
    logger.info("Cargando 'commune_summary' a Redis...")
    lines_comuna = get_hdfs_data(HDFS_RESULTS_PATHS["commune_summary"])
    if lines_comuna:
        pipe = r_client.pipeline()
        redis_key_comuna = "stats:commune_summary"
        pipe.delete(redis_key_comuna) # Limpiar clave anterior (Hash)
        for line in lines_comuna:
            try:
                data = json.loads(line)
                comuna = data.get('commune')
                total_incidents = data.get('total_incidents')
                if comuna and total_incidents is not None:
                    pipe.hset(redis_key_comuna, comuna, int(total_incidents))
                else:
                    logger.warning(f"Faltan campos (commune o total_incidents) en línea: {line}")
            except json.JSONDecodeError:
                logger.warning(f"Saltando línea malformada (JSON - commune_summary): {line}")
            except ValueError:
                logger.warning(f"Saltando línea malformada (Valor - commune_summary): {line}")
        pipe.execute()
        logger.info(f"Cargados datos de 'commune_summary' a Redis (clave: {redis_key_comuna}).")
        success_count += 1
    else:
        logger.warning("No hay datos de 'commune_summary' para cargar.")


    # 2. Cargar 'type_summary' a Redis
    logger.info("Cargando 'type_summary' a Redis...")
    lines_type = get_hdfs_data(HDFS_RESULTS_PATHS["type_summary"])
    if lines_type:
        pipe = r_client.pipeline()
        redis_key_type = "stats:type_summary"
        pipe.delete(redis_key_type) # Limpiar clave anterior (Hash)
        for line in lines_type:
            try:
                data = json.loads(line)
                standardized_type = data.get('standardized_type')
                total_occurrences = data.get('total_occurrences')
                if standardized_type and total_occurrences is not None:
                    pipe.hset(redis_key_type, standardized_type, int(total_occurrences))
                else:
                    logger.warning(f"Faltan campos (standardized_type o total_occurrences) en línea: {line}")
            except json.JSONDecodeError:
                logger.warning(f"Saltando línea malformada (JSON - type_summary): {line}")
            except ValueError:
                logger.warning(f"Saltando línea malformada (Valor - type_summary): {line}")
        pipe.execute()
        logger.info(f"Cargados datos de 'type_summary' a Redis (clave: {redis_key_type}).")
        success_count += 1
    else:
        logger.warning("No hay datos de 'type_summary' para cargar.")


    # 3. Cargar 'daily_summary' a Redis (ejemplo con SET para claves únicas)
    logger.info("Cargando 'daily_summary' a Redis...")
    lines_daily = get_hdfs_data(HDFS_RESULTS_PATHS["daily_summary"])
    if lines_daily:
        pipe = r_client.pipeline()
        redis_key_daily_base = "stats:daily_summary"
        # Limpiar todas las claves que empiecen con este patrón para evitar duplicados en cada ejecución
        for key in r_client.keys(f"{redis_key_daily_base}:*"):
            pipe.delete(key)
        pipe.execute() # Ejecutar las eliminaciones primero

        pipe = r_client.pipeline() # Nuevo pipeline para las inserciones
        for line in lines_daily:
            try:
                data = json.loads(line)
                event_date = data.get('event_date')
                standardized_type = data.get('standardized_type')
                commune = data.get('commune')
                incidents_count = data.get('incidents_count')

                if all(x is not None for x in [event_date, standardized_type, commune, incidents_count]):
                    # Clave: stats:daily_summary:2024-06-02:Accidente:Las_Condes
                    specific_key = f"{redis_key_daily_base}:{event_date}:{standardized_type.replace(' ','_')}:{commune.replace(' ','_')}"
                    pipe.set(specific_key, int(incidents_count)) # Usar SET directo si es un solo valor por clave única
                else:
                    logger.warning(f"Faltan campos (daily_summary) en línea: {line}")
            except json.JSONDecodeError:
                logger.warning(f"Saltando línea malformada (JSON - daily_summary): {line}")
            except ValueError:
                logger.warning(f"Saltando línea malformada (Valor - daily_summary): {line}")
        pipe.execute()
        logger.info(f"Cargados datos de 'daily_summary' a Redis (claves base: {redis_key_daily_base}:*).")
        success_count += 1
    else:
        logger.warning("No hay datos de 'daily_summary' para cargar.")

    # 4. Cargar 'hourly_summary' a Redis
    logger.info("Cargando 'hourly_summary' a Redis...")
    lines_hourly = get_hdfs_data(HDFS_RESULTS_PATHS["hourly_summary"])
    if lines_hourly:
        pipe = r_client.pipeline()
        redis_key_hourly_base = "stats:hourly_summary"
        # Limpiar todas las claves que empiecen con este patrón
        for key in r_client.keys(f"{redis_key_hourly_base}:*"):
            pipe.delete(key)
        pipe.execute() # Ejecutar las eliminaciones primero

        pipe = r_client.pipeline() # Nuevo pipeline para las inserciones
        for line in lines_hourly:
            try:
                data = json.loads(line)
                event_hour = data.get('event_hour')
                standardized_type = data.get('standardized_type')
                commune = data.get('commune')
                incidents_count = data.get('incidents_count')

                if all(x is not None for x in [event_hour, standardized_type, commune, incidents_count]):
                    # Clave: stats:hourly_summary:09:Accidente:Las_Condes
                    specific_key = f"{redis_key_hourly_base}:{event_hour}:{standardized_type.replace(' ','_')}:{commune.replace(' ','_')}"
                    pipe.set(specific_key, int(incidents_count))
                else:
                    logger.warning(f"Faltan campos (hourly_summary) en línea: {line}")
            except json.JSONDecodeError:
                logger.warning(f"Saltando línea malformada (JSON - hourly_summary): {line}")
            except ValueError:
                logger.warning(f"Saltando línea malformada (Valor - hourly_summary): {line}")
        pipe.execute()
        logger.info(f"Cargados datos de 'hourly_summary' a Redis (claves base: {redis_key_hourly_base}:*).")
        success_count += 1
    else:
        logger.warning("No hay datos de 'hourly_summary' para cargar.")
    
    return success_count > 0 # Retorna True si al menos un tipo de dato fue cargado


if __name__ == "__main__":
    logger.info("--- Iniciando script de carga de resultados de Pig a Redis ---")
    redis_client = connect_to_redis()
    if not redis_client:
        sys.exit(1)

    try:
        if load_to_redis(redis_client):
            logger.info("Carga de datos a Redis finalizada exitosamente.")
        else:
            logger.warning("Carga de datos a Redis finalizada. No se cargó ningún tipo de dato.")
            sys.exit(0) # No es un error crítico si no hay datos de Pig aún
    except Exception as e:
        logger.exception(f"Error inesperado durante la carga a Redis: {e}")
        sys.exit(1)
    finally:
        if redis_client:
            redis_client.close()
            logger.info("Conexión a Redis cerrada.")
    logger.info("--- Script de carga a Redis finalizado ---")

import sys
import os
import random
import time
import logging
import math
import numpy as np # Necesario para distribuciones de tiempo

# --- Dependencias ---
try:
    from pymongo import MongoClient
    from pymongo.errors import ConnectionFailure, ServerSelectionTimeoutError
except ModuleNotFoundError:
    print("ERROR: El módulo 'pymongo' no está instalado. Ejecuta: pip install pymongo")
    sys.exit(1)
try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError
except ModuleNotFoundError:
    print("ERROR: El módulo 'redis' no está instalado. Ejecuta: pip install redis")
    sys.exit(1)
try:
    import numpy as np
except ModuleNotFoundError:
    print("ERROR: El módulo 'numpy' no está instalado. Ejecuta: pip install numpy")
    sys.exit(1)

# --- Configuración de Logging ---
print("--- Script traffic_generator.py iniciado ---", flush=True)
print("--- Dependencias importadas, configurando logger... ---", flush=True)

logging.basicConfig(level=logging.INFO, # Nivel de log por defecto
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger()
logger.setLevel(logging.INFO) # Asegurarse de que el logger principal esté en INFO o superior

RESULTS_LOG_FILENAME = "generator_results.log"
try:
    # Se usa un logger independiente para los resultados finales para que no se mezclen
    # con los logs de ejecución y puedan ser parseados fácilmente.
    results_formatter = logging.Formatter('%(asctime)s - CONF: %(redis_config)s - ARRIVAL: %(arrival_dist)s - RPS: %(avg_rps).2f - HITS: %(hits)s - MISSES: %(misses)s - HIT_RATE: %(hit_rate).2f%%')
    results_handler = logging.FileHandler(RESULTS_LOG_FILENAME, encoding='utf-8', mode='a') # Modo append
    results_handler.setFormatter(results_formatter)
    
    class ResultsFilter(logging.Filter):
        def filter(self, record):
            return hasattr(record, 'is_result') and record.is_result
    results_handler.addFilter(ResultsFilter())
    logger.addHandler(results_handler)
    print(f"--- Resultados se añadirán a {RESULTS_LOG_FILENAME} ---", flush=True)
except Exception as e:
    print(f"--- ERROR configurando el log de resultados: {e} ---", flush=True)

print("--- Logger configurado ---", flush=True)


# --- Configuración ---
MONGO_HOST = os.getenv('MONGO_HOST', 'mongo_db')
MONGO_URI = f"mongodb://{MONGO_HOST}:27017/"
DATABASE_NAME = "waze_data"
COLLECTION_NAME = "events"

REDIS_HOST = os.getenv('REDIS_HOST', 'redis_cache')
REDIS_PORT = int(os.getenv('REDIS_PORT', 6379))

ARRIVAL_MODE = os.getenv('ARRIVAL_MODE', 'poisson').lower()
NUM_REQUESTS = int(os.getenv('NUM_REQUESTS', '50000'))
REDIS_CONFIG_NAME = os.getenv('REDIS_CONFIG_FILE', 'CONFIG_NO_ESPECIFICADA')
TOTAL_SIMULATION_DURATION_SECONDS = int(os.getenv('SIMULATION_DURATION', '60'))

# --- NUEVAS VARIABLES DE ESPERA DE DATOS ---
MIN_EVENTS_THRESHOLD = 500 # Mínimo de eventos requeridos antes de iniciar la simulación
DATA_AVAILABILITY_RETRIES = 120 # Número de intentos para esperar por los datos (120 * 10s = 20 minutos)
DATA_AVAILABILITY_DELAY = 10 # Retardo entre cada intento (en segundos)

def get_event_ids_from_mongo():
    """
    Obtiene una lista de event_id únicos desde MongoDB, esperando hasta que haya suficientes datos.
    """
    event_ids = []
    client = None
    
    logger.info(f"Obteniendo IDs únicos de eventos desde MongoDB ({MONGO_HOST})...")
    print(f"--- Intentando conectar a MongoDB en {MONGO_URI}... ---", flush=True)

    # Bucle para esperar por la conexión a MongoDB y la disponibilidad de datos
    for attempt in range(DATA_AVAILABILITY_RETRIES):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=5000) # Timeout de conexión más corto
            client.admin.command('ping') # Prueba la conexión
            logger.info(f"Conexión a MongoDB exitosa (Intento {attempt+1}).")
            
            db = client[DATABASE_NAME]
            collection = db[COLLECTION_NAME]
            
            # Usar estimated_document_count para una estimación rápida si la colección es grande
            # O count_documents({}) si necesitas un conteo exacto (puede ser más lento en colecciones grandes)
            current_event_count = collection.estimated_document_count() 
            
            logger.info(f"Intento {attempt+1}/{DATA_AVAILABILITY_RETRIES}: Se encontraron {current_event_count} eventos en MongoDB.")
            
            if current_event_count >= MIN_EVENTS_THRESHOLD:
                logger.info(f"Umbral de {MIN_EVENTS_THRESHOLD} eventos alcanzado. Procediendo a obtener IDs.")
                # Asegúrate de que `event_id` sea un campo válido en tus documentos de Waze.
                # Si el campo es `_id`, cambia `collection.distinct("event_id")` a `collection.distinct("_id")`
                event_ids = collection.distinct("event_id") 
                logger.info(f"Se obtuvieron {len(event_ids)} event_ids únicos desde MongoDB.")
                print(f"--- Número de IDs únicos encontrados: {len(event_ids)} ---", flush=True)
                client.close()
                return event_ids # Retorna los IDs y termina la función
            else:
                logger.info(f"Esperando a tener al menos {MIN_EVENTS_THRESHOLD} eventos. Faltan: {MIN_EVENTS_THRESHOLD - current_event_count}. Reintentando en {DATA_AVAILABILITY_DELAY}s...")
                client.close() # Cierra la conexión actual antes de esperar
                time.sleep(DATA_AVAILABILITY_DELAY)

        except (ConnectionFailure, ServerSelectionTimeoutError) as e:
            logger.warning(f"Error de conexión a MongoDB (Intento {attempt+1}/{DATA_AVAILABILITY_RETRIES}): {e}. Reintentando en {DATA_AVAILABILITY_DELAY}s...")
            print(f"--- ERROR Intento {attempt+1} conexión MongoDB: {e} ---", flush=True)
            if client: client.close() # Asegurarse de cerrar si la conexión falló a mitad
            time.sleep(DATA_AVAILABILITY_DELAY)
        except Exception as e:
            logger.exception(f"Error inesperado al conectar o leer de MongoDB (Intento {attempt+1}): {e}. Reintentando en {DATA_AVAILABILITY_DELAY}s...")
            if client: client.close()
            time.sleep(DATA_AVAILABILITY_DELAY)
    
    # Si el bucle termina sin retornar (es decir, los reintentos se agotaron)
    logger.error(f"No se pudieron obtener suficientes IDs únicos de eventos ({MIN_EVENTS_THRESHOLD}) desde MongoDB después de {DATA_AVAILABILITY_RETRIES} intentos. Abortando.")
    print("--- Fallo al obtener IDs, abortando. ---", flush=True)
    sys.exit(1) # Salir con un código de error si no hay datos

def simulate_traffic(event_ids):
    """Simula el tráfico de consultas hacia Redis con distribución temporal."""
    logger.info(f"Iniciando simulación: Config={REDIS_CONFIG_NAME}, Modo Llegada='{ARRIVAL_MODE}', Consultas={NUM_REQUESTS}, Duración Objetivo={TOTAL_SIMULATION_DURATION_SECONDS}s")
    logger.info(f"Conectando a Redis en {REDIS_HOST}:{REDIS_PORT}...")
    print(f"--- Intentando conectar a Redis en {REDIS_HOST}:{REDIS_PORT}... ---", flush=True)

    try:
        redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
        redis_conn.ping()
        logger.info("Conexión a Redis exitosa.")
        print("--- Conexión a Redis OK ---", flush=True)
    except RedisConnectionError as e:
        logger.error(f"Error de conexión a Redis: {e}")
        print(f"--- ERROR de conexión a Redis: {e} ---", flush=True)
        sys.exit(1)

    hits = 0
    misses = 0
    oom_errors = 0 # Contador de errores OOM
    total_unique_ids = len(event_ids)

    if TOTAL_SIMULATION_DURATION_SECONDS <= 0:
        logger.error("La duración de la simulación debe ser mayor que cero.")
        sys.exit(1)
    average_requests_per_second = NUM_REQUESTS / TOTAL_SIMULATION_DURATION_SECONDS
    logger.info(f"Tasa promedio objetivo: {average_requests_per_second:.2f} req/s")

    if ARRIVAL_MODE == "poisson":
        lambda_rate = average_requests_per_second
        if lambda_rate <= 0: logger.error("Lambda para Poisson debe ser > 0."); sys.exit(1)
    elif ARRIVAL_MODE == "jittered":
        if average_requests_per_second <= 0: logger.error("Tasa promedio para Jittered debe ser > 0."); sys.exit(1)
        average_inter_arrival_time = 1.0 / average_requests_per_second
        jitter_factor = 0.3
        min_inter_arrival = max(0.0001, average_inter_arrival_time * (1 - jitter_factor))
        max_inter_arrival = average_inter_arrival_time * (1 + jitter_factor)
        logger.info(f"Tiempo promedio entre llegadas (Jittered): {average_inter_arrival_time:.4f}s (rango: {min_inter_arrival:.4f}s - {max_inter_arrival:.4f}s)")
    else:
        logger.error(f"Modo de llegada '{ARRIVAL_MODE}' no reconocido."); sys.exit(1)

    logger.info(f"Comenzando simulación de {NUM_REQUESTS} consultas...")
    print(f"--- Iniciando bucle de simulación ({NUM_REQUESTS} consultas)... ---", flush=True)
    start_time = time.time()
    requests_processed = 0

    while requests_processed < NUM_REQUESTS:
        # --- Timeout de seguridad para el bucle ---
        current_duration = time.time() - start_time
        if current_duration > TOTAL_SIMULATION_DURATION_SECONDS * 1.5: # Buffer del 50%
            logger.error(f"Error: Simulación excedió el tiempo límite de {TOTAL_SIMULATION_DURATION_SECONDS * 1.5:.0f} segundos. Abortando.")
            break

        wait_time = 0
        if ARRIVAL_MODE == "poisson": wait_time = random.expovariate(lambda_rate)
        elif ARRIVAL_MODE == "jittered": wait_time = random.uniform(min_inter_arrival, max_inter_arrival)
        if wait_time > 0: time.sleep(wait_time)

        if not event_ids: logger.warning("Lista de IDs vacía durante la simulación."); break
        event_id = random.choice(event_ids)
        redis_key = f"event:{event_id}"

        try:
            if redis_conn.exists(redis_key):
                hits += 1
            else:
                misses += 1
                try:
                    redis_conn.set(redis_key, "1")
                except ResponseError as re:
                    error_string = str(re)
                    if "OOM command not allowed" in error_string or "used memory > 'maxmemory'" in error_string:
                        oom_errors += 1
                        logger.info(f"  OOM Error al intentar SET para {redis_key}. Caché lleno (Miss contado).")
                    else:
                        logger.error(f"Error de respuesta inesperado de Redis durante SET para {redis_key}: {re}")
                except RedisConnectionError as e_set:
                     logger.error(f"Error de conexión a Redis durante SET: {e_set}. Intentando reconectar...")
                     try:
                         redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
                         redis_conn.ping()
                         logger.info("Reconexión a Redis exitosa.")
                     except RedisConnectionError:
                         logger.error("Fallo al reconectar a Redis. Abortando simulación.")
                         break
                     except Exception as e_recon_set:
                         logger.exception(f"Error inesperado durante reconexión (post-SET fail): {e_recon_set}")
                except Exception as set_err:
                     logger.exception(f"Error inesperado durante SET para {redis_key}: {set_err}")


        except RedisConnectionError as e:
            logger.error(f"Error de conexión a Redis durante EXISTS: {e}. Reintentando...")
            try:
                redis_conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=0, decode_responses=True)
                redis_conn.ping()
                logger.info("Reconexión a Redis exitosa.")
            except RedisConnectionError:
                logger.error("Fallo al reconectar a Redis. Abortando simulación.")
                break
            except Exception as e_recon_exists:
                 logger.exception(f"Error inesperado durante reconexión (post-EXISTS fail): {e_recon_exists}")

        except Exception as e:
            logger.exception(f"Error inesperado durante consulta a Redis para {redis_key}: {e}")

        requests_processed += 1

        if requests_processed % (NUM_REQUESTS // 20 or 1) == 0:
             progress = requests_processed / NUM_REQUESTS * 100
             elapsed_time = time.time() - start_time
             current_rps = requests_processed / elapsed_time if elapsed_time > 0 else 0
             logger.info(f"  Progreso: {progress:.0f}% ({requests_processed}/{NUM_REQUESTS}) - RPS actual: {current_rps:.2f}")

    end_time = time.time()
    actual_duration = end_time - start_time
    actual_avg_rps = requests_processed / actual_duration if actual_duration > 0 else 0

    logger.info(f"Simulación finalizada en {actual_duration:.2f} segundos.")
    logger.info(f"Consultas intentadas/procesadas: {requests_processed}")
    logger.info(f"Tasa promedio lograda: {actual_avg_rps:.2f} req/s")
    logger.info(f"Hits: {hits}")
    logger.info(f"Misses: {misses}")
    if oom_errors > 0:
        logger.warning(f"Errores OOM (Out Of Memory) encontrados: {oom_errors}")

    effective_queries = hits + misses
    if effective_queries > 0:
        hit_rate = (hits / effective_queries) * 100
        miss_rate = (misses / effective_queries) * 100
        log_extra_data = {
            'redis_config': REDIS_CONFIG_NAME,
            'arrival_dist': ARRIVAL_MODE.upper(),
            'avg_rps': actual_avg_rps,
            'hits': hits,
            'misses': misses,
            'hit_rate': hit_rate
        }
        logger.warning("Resultado final de la simulación", extra={'is_result': True, **log_extra_data})
        print(f"\n=== RESULTADO FINAL ===")
        print(f"Config Redis: {REDIS_CONFIG_NAME}")
        print(f"Modo Llegada: {ARRIVAL_MODE.upper()}")
        print(f"RPS Promedio Logrado: {actual_avg_rps:.2f}")
        print(f"Hits: {hits}")
        print(f"Misses: {misses}")
        print(f"Errores OOM: {oom_errors}")
        print(f"Hit Rate (calculado sobre H+M): {hit_rate:.2f}%")
        print(f"Miss Rate (calculado sobre H+M): {miss_rate:.2f}%")
        print(f"=====================")
    else:
        logger.warning("No se procesaron consultas efectivas (hits/misses).")

if __name__ == "__main__":
    print("--- Entrando a __main__, llamando a get_event_ids_from_mongo... ---", flush=True)
    event_ids_list = get_event_ids_from_mongo()
    # Si get_event_ids_from_mongo no sale con sys.exit(1), significa que encontró IDs
    print(f"--- get_event_ids_from_mongo retornó. Lista tiene {len(event_ids_list)} elementos. ---", flush=True)

    if event_ids_list: # Vuelve a verificar que la lista no esté vacía por si acaso
        print("--- Llamando a simulate_traffic... ---", flush=True)
        simulate_traffic(event_ids_list)
        print("--- simulate_traffic finalizado. ---", flush=True)
    else:
        # Esto no debería pasar si get_event_ids_from_mongo funciona como se espera
        logger.error("La lista de IDs de eventos está vacía después de la espera. Abortando.")
        sys.exit(1)

    print("--- Script traffic_generator.py finalizado. ---", flush=True)
    logging.shutdown()

print("--- Script traffic_generator.py iniciado ---", flush=True)

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
    from pymongo.errors import ConnectionFailure
except ModuleNotFoundError:
    print("ERROR: El módulo 'pymongo' no está instalado. Ejecuta: pip install pymongo")
    sys.exit(1)
try:
    import redis
    # Importar ResponseError
    from redis.exceptions import ConnectionError as RedisConnectionError, ResponseError
except ModuleNotFoundError:
    print("ERROR: El módulo 'redis' no está instalado. Ejecuta: pip install redis")
    sys.exit(1)
try:
    import numpy as np
except ModuleNotFoundError:
    print("ERROR: El módulo 'numpy' no está instalado. Ejecuta: pip install numpy")
    sys.exit(1)


print("--- Dependencias importadas, configurando logger... ---", flush=True)

# --- Configuración de Logging ---
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(levelname)s - %(message)s',
                    stream=sys.stdout)
logger = logging.getLogger()

RESULTS_LOG_FILENAME = "generator_results.log"
try:
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

MONGO_CONNECT_RETRIES = 5
MONGO_CONNECT_DELAY = 5

def get_event_ids_from_mongo():
    """Obtiene una lista de event_id únicos desde MongoDB."""
    ids = []
    client = None
    logger.info(f"Obteniendo IDs únicos de eventos desde MongoDB ({MONGO_HOST})...")
    print(f"--- Intentando conectar a MongoDB en {MONGO_URI}... ---", flush=True)

    for attempt in range(MONGO_CONNECT_RETRIES):
        try:
            client = MongoClient(MONGO_URI, serverSelectionTimeoutMS=10000)
            client.admin.command('ping')
            logger.info(f"Conexión a MongoDB exitosa (Intento {attempt+1}).")
            print(f"--- Conexión a MongoDB OK (Intento {attempt+1}) ---", flush=True)
            break
        except ConnectionFailure as e:
            logger.warning(f"Intento {attempt+1} fallido: {e}")
            print(f"--- ERROR Intento {attempt+1} conexión MongoDB: {e} ---", flush=True)
            if attempt < MONGO_CONNECT_RETRIES - 1:
                logger.info(f"Esperando {MONGO_CONNECT_DELAY}s...")
                time.sleep(MONGO_CONNECT_DELAY)
            else:
                logger.error("Máximos reintentos de conexión a MongoDB alcanzados.")
                sys.exit(1)
        except Exception as e:
             logger.exception(f"Error inesperado conectando a MongoDB (Intento {attempt+1}): {e}")
             if attempt < MONGO_CONNECT_RETRIES - 1: time.sleep(MONGO_CONNECT_DELAY)
             else: logger.error("Error inesperado persistente."); sys.exit(1)

    if not client: sys.exit(1)

    try:
        db = client[DATABASE_NAME]
        collection = db[COLLECTION_NAME]
        logger.info(f"Leyendo 'event_id' desde {DATABASE_NAME}.{COLLECTION_NAME}...")
        ids = collection.distinct("event_id")
        logger.info(f"Se obtuvieron {len(ids)} event_ids únicos desde MongoDB.")
        print(f"--- Número de IDs únicos encontrados: {len(ids)} ---", flush=True)
    except Exception as e:
        logger.exception(f"Error inesperado obteniendo IDs de MongoDB: {e}")
        print(f"--- ERROR inesperado obteniendo IDs: {e} ---", flush=True)
        ids = []
    finally:
         if client:
              logger.info("Cerrando conexión a MongoDB.")
              client.close()

    if not ids:
        logger.error("No se pudieron obtener IDs únicos de eventos desde MongoDB. Abortando.")
        print("--- Lista de IDs vacía, abortando. ---", flush=True)
        sys.exit(1)
    return ids

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
                    # --- CAMBIO: Verificar ambas cadenas de error OOM ---
                    error_string = str(re)
                    if "OOM command not allowed" in error_string or "used memory > 'maxmemory'" in error_string:
                        oom_errors += 1
                        # Loguear solo a nivel INFO para reducir ruido
                        logger.info(f"  OOM Error al intentar SET para {redis_key}. Caché lleno (Miss contado).")
                    else:
                        # Otro tipo de ResponseError durante el SET
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
                except Exception as set_err: # Capturar otros errores del SET
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

        # Incrementar contador SIEMPRE al final de la iteración
        requests_processed += 1

        if requests_processed % (NUM_REQUESTS // 20 or 1) == 0:
             progress = requests_processed / NUM_REQUESTS * 100
             elapsed_time = time.time() - start_time
             current_rps = requests_processed / elapsed_time if elapsed_time > 0 else 0
             logger.info(f"  Progreso: {progress:.0f}% ({requests_processed}/{NUM_REQUESTS}) - RPS actual: {current_rps:.2f}")

    # --- Fin del bucle while ---
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
    print(f"--- get_event_ids_from_mongo retornó. Lista tiene {len(event_ids_list)} elementos. ---", flush=True)

    if event_ids_list:
        print("--- Llamando a simulate_traffic... ---", flush=True)
        simulate_traffic(event_ids_list)
        print("--- simulate_traffic finalizado. ---", flush=True)
    else:
        pass

    print("--- Script traffic_generator.py finalizado. ---", flush=True)
    logging.shutdown()
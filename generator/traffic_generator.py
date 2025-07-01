#!/usr/bin/env python3
"""
Generador de tráfico para simular consultas al caché Redis
Útil para demostrar métricas de hit/miss y rendimiento del sistema de caché
"""
import sys
import os
import random
import time
import logging
import json
from datetime import datetime

try:
    import redis
    from redis.exceptions import ConnectionError as RedisConnectionError
except ModuleNotFoundError:
    sys.exit("FATAL: 'redis' no está instalado.")

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', stream=sys.stdout)
logger = logging.getLogger()

REDIS_HOST = os.getenv('REDIS_HOST', 'cache')
SIMULATION_DURATION = 120

def connect_to_redis():
    for attempt in range(12):
        try:
            r = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            r.ping()
            logger.info(f"Conexión a Redis en {REDIS_HOST} exitosa.")
            return r
        except RedisConnectionError as e:
            logger.warning(f"Error de conexión a Redis: {e}. Reintentando...")
            time.sleep(10)
    logger.error("No se pudo conectar a Redis.")
    sys.exit(1)

def wait_for_data_in_redis(redis_conn):
    logger.info("Esperando a que haya datos de eventos en Redis...")
    for _ in range(60):
        # Buscamos claves que coincidan con los eventos organizados por criterios
        keys = redis_conn.keys("events:*")
        if len(keys) > 3: # Esperar a que haya al menos algunos tipos de criterios
            logger.info(f"Datos de eventos encontrados en Redis ({len(keys)} claves).")
            return keys
        time.sleep(10)
    logger.error("No se encontraron datos de eventos en Redis después de esperar. Abortando.")
    sys.exit(1)

def get_sample_queries(redis_conn):
    """Obtiene consultas de ejemplo basadas en los datos disponibles"""
    queries = []
    
    # Consultas por comuna (populares)
    commune_keys = redis_conn.keys("events:commune:*")
    queries.extend(commune_keys[:3])  # Top 3 comunas
    
    # Consultas por tipo (frecuentes)
    type_keys = redis_conn.keys("events:type:*")
    queries.extend(type_keys)
    
    # Consultas por hora (menos frecuentes)
    hour_keys = redis_conn.keys("events:hour:*")
    queries.extend(hour_keys[:5])  # Algunas horas
    
    # Consultas especiales
    special_queries = [
        "events:recent:last_100",
        "events:stats:general"
    ]
    queries.extend(special_queries)
    
    return queries

def simulate_realistic_traffic(query_keys, redis_conn, distribution, rate):
    logger.info(f"--- Iniciando Simulación de Tráfico de Consultas ---")
    logger.info(f"Distribución: {distribution.upper()}, Tasa (RPS): {rate}, Duración: {SIMULATION_DURATION}s")
    
    hits, misses = 0, 0
    start_time = time.time()
    
    # Separar consultas por popularidad (80/20)
    popular_queries = [q for q in query_keys if 'commune' in q or 'type' in q]
    rare_queries = [q for q in query_keys if 'hour' in q or 'stats' in q or 'recent' in q]
    
    while time.time() - start_time < SIMULATION_DURATION:
        if distribution == 'poisson':
            wait_time = random.expovariate(rate)
        else: # Uniforme
            wait_time = 1.0 / rate
        
        time.sleep(wait_time)
        
        # 80% consultas populares, 20% consultas raras
        if random.random() < 0.8 and popular_queries:
            query_key = random.choice(popular_queries)
        elif rare_queries:
            query_key = random.choice(rare_queries)
        else:
            query_key = random.choice(query_keys)
        
        try:
            start_query = time.time()
            result = redis_conn.get(query_key)
            end_query = time.time()
            
            response_time = (end_query - start_query) * 1000  # en milisegundos
            
            if result:
                hits += 1
                # Contar eventos en el resultado
                try:
                    data = json.loads(result)
                    if isinstance(data, list):
                        event_count = len(data)
                    else:
                        event_count = 1
                    logger.info(f"🟢 CACHE HIT: {query_key} | {response_time:.2f}ms | {event_count} eventos")
                except:
                    logger.info(f"🟢 CACHE HIT: {query_key} | {response_time:.2f}ms")
            else:
                misses += 1
                logger.info(f"🔴 CACHE MISS: {query_key} | {response_time:.2f}ms | Consultando Elasticsearch...")
                # Simular consulta a Elasticsearch (más lenta)
                time.sleep(0.1)
        except Exception as e:
            logger.error(f"Error inesperado en simulación: {e}")

    total_queries = hits + misses
    hit_rate = (hits / total_queries * 100) if total_queries > 0 else 0
    
    logger.info(f"--- Simulación Finalizada ({distribution.upper()}) ---")
    logger.info(f"Consultas Totales: {total_queries}, Hits: {hits}, Misses: {misses}")
    logger.info(f"Hit Rate: {hit_rate:.2f}%")
    logger.info(f"Tiempo Promedio de Respuesta: {(SIMULATION_DURATION * 1000 / total_queries):.2f}ms")

if __name__ == "__main__":
    redis_conn = connect_to_redis()
    query_keys = wait_for_data_in_redis(redis_conn)
    
    # Obtener consultas realistas basadas en los datos disponibles
    sample_queries = get_sample_queries(redis_conn)
    
    logger.info(f"Consultas disponibles para simular: {len(sample_queries)}")
    for query in sample_queries[:5]:  # Mostrar algunas de ejemplo
        logger.info(f"  - {query}")
    
    simulate_realistic_traffic(sample_queries, redis_conn, 'poisson', rate=10)
    logger.info("\nCambiando a distribución Uniforme en 10 segundos...\n")
    time.sleep(10)
    simulate_realistic_traffic(sample_queries, redis_conn, 'uniform', rate=10)

    logger.info("--- Generador de tráfico ha finalizado. ---")
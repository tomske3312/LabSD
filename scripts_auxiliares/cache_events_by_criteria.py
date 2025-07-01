#!/usr/bin/env python3
"""
Carga eventos individuales en Redis organizados por criterios de consulta frecuente
"""

import json
import redis
import logging
from pathlib import Path
import subprocess
import os
from datetime import datetime
import time

# Configuración de logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configuración
REDIS_HOST = os.getenv('REDIS_HOST', 'localhost')
HDFS_PATH = '/user/hadoop/waze_processed/individual_events'
LOCAL_TEMP_PATH = '/tmp/individual_events_data'

def connect_to_redis():
    """Conecta a Redis con reintentos."""
    for i in range(10):
        try:
            r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=True)
            r.ping()
            logger.info("Conexión a Redis exitosa.")
            return r
        except Exception as e:
            logger.warning(f"Intento {i+1}/10 de conexión a Redis falló: {e}")
            time.sleep(2)
    logger.error("No se pudo conectar a Redis.")
    return None

def download_hdfs_data():
    """Descarga datos de HDFS al sistema local."""
    try:
        # Limpiar directorio temporal si existe
        import shutil
        if os.path.exists(LOCAL_TEMP_PATH):
            shutil.rmtree(LOCAL_TEMP_PATH)
        
        # Crear directorio temporal
        os.makedirs(os.path.dirname(LOCAL_TEMP_PATH), exist_ok=True)
        
        # Descargar de HDFS
        subprocess.run([
            'hdfs', 'dfs', '-get', HDFS_PATH, LOCAL_TEMP_PATH
        ], check=True)
        
        logger.info(f"Datos descargados de HDFS a {LOCAL_TEMP_PATH}")
        return True
    except subprocess.CalledProcessError as e:
        logger.error(f"Error descargando de HDFS: {e}")
        return False

def parse_event_line(line):
    """Convierte una línea TSV en un diccionario de evento."""
    fields = line.strip().split('\t')
    if len(fields) < 11:
        return None
    
    try:
        return {
            'event_id': fields[0],
            'type_original': fields[1],
            'address': fields[2],
            'report_time': fields[3],
            'latitude': float(fields[4]) if fields[4] and fields[4] != 'null' else None,
            'longitude': float(fields[5]) if fields[5] and fields[5] != 'null' else None,
            'confidence': int(fields[6]) if fields[6] and fields[6] != 'null' else 0,
            'reporter': fields[7],
            'sector': fields[8] if fields[8] and fields[8] != 'null' else 'Desconocido',
            'calle': fields[9] if fields[9] and fields[9] != 'null' else '',
            'tipo_evento': fields[10],
            'hora_reporte': fields[11] if len(fields) > 11 and fields[11] != 'null' else None
        }
    except (ValueError, IndexError) as e:
        logger.warning(f"Error parseando línea: {e}")
        return None

def load_events_from_hdfs():
    """Carga eventos desde los archivos de HDFS."""
    if not download_hdfs_data():
        return []
    
    events = []
    
    # Leer todos los archivos part-*
    hdfs_dir = Path(LOCAL_TEMP_PATH)
    for part_file in hdfs_dir.glob('part-*'):
        try:
            with open(part_file, 'r', encoding='utf-8') as f:
                for line in f:
                    event = parse_event_line(line)
                    if event:
                        events.append(event)
        except Exception as e:
            logger.error(f"Error leyendo archivo {part_file}: {e}")
    
    logger.info(f"Cargados {len(events)} eventos desde HDFS")
    return events

def cache_events_by_criteria(redis_client, events):
    """Organiza eventos en Redis por diferentes criterios (SIMPLIFICADO)."""
    
    start_time = time.time()  # Iniciar cronómetro
    cache_operations = 0
    
    # 1. Cachear TODOS los eventos (siempre útil)
    logger.info("Cacheando todos los eventos...")
    redis_client.set("events:all", json.dumps(events))
    cache_operations += 1
    
    # 2. Organizar por sector (solo los que tienen más de 5 eventos)
    logger.info("Cacheando eventos por sector...")
    events_by_sector = {}
    for event in events:
        sector = event.get('sector', 'Desconocido')
        if sector and sector != 'Desconocido':
            if sector not in events_by_sector:
                events_by_sector[sector] = []
            events_by_sector[sector].append(event)
    
    # Solo cachear sectores con suficientes datos
    for sector, sector_events in events_by_sector.items():
        if len(sector_events) >= 5:  # Solo sectores con al menos 5 eventos
            redis_client.set(
                f"events:sector:{sector.lower().replace(' ', '_')}", 
                json.dumps(sector_events)
            )
            cache_operations += 1
    
    # 3. Organizar por tipo de evento (siempre útil)
    logger.info("Cacheando eventos por tipo...")
    events_by_type = {}
    for event in events:
        tipo = event.get('tipo_evento', 'Otro')
        if tipo not in events_by_type:
            events_by_type[tipo] = []
        events_by_type[tipo].append(event)
    
    for tipo, tipo_events in events_by_type.items():
        redis_client.set(
            f"events:type:{tipo.lower().replace(' ', '_')}", 
            json.dumps(tipo_events)
        )
        cache_operations += 1
    
    # 4. Cachear eventos recientes (últimos 100)
    logger.info("Cacheando eventos recientes...")
    recent_events = events[:100]  # Los primeros 100 del procesamiento
    redis_client.set("events:recent", json.dumps(recent_events))
    cache_operations += 1
    
    # 5. Estadísticas generales con métricas de caché
    sectores_con_datos = [s for s, events in events_by_sector.items() if len(events) >= 5]
    end_time = time.time()
    elapsed_time = end_time - start_time
    
    stats = {
        'total_events': len(events),
        'sectores_principales': sectores_con_datos,
        'tipos_evento': list(events_by_type.keys()),
        'cache_updated': datetime.now().isoformat(),
        'cache_operations': cache_operations,
        'processing_time_seconds': round(elapsed_time, 2),
        'events_per_second': round(len(events) / elapsed_time, 2) if elapsed_time > 0 else 0
    }
    redis_client.set("events:stats", json.dumps(stats))
    cache_operations += 1
    
    end_time = time.time()  # Detener cronómetro
    elapsed_time = end_time - start_time
    logger.info(f"Tiempo de procesamiento: {elapsed_time:.2f} segundos")
    
    logger.info(f"📊 MÉTRICAS DE CACHÉ:")
    logger.info(f"⏱️  Tiempo de procesamiento: {elapsed_time:.2f} segundos")
    logger.info(f"⚡ Eventos por segundo: {round(len(events) / elapsed_time, 2) if elapsed_time > 0 else 0}")
    logger.info(f"🔄 Operaciones de caché realizadas: {cache_operations}")
    logger.info(f"📈 Cache actualizado con {len(events)} eventos")
    logger.info(f"🏘️  Sectores principales ({len(sectores_con_datos)}): {sectores_con_datos}")
    logger.info(f"🚨 Tipos de evento: {list(events_by_type.keys())}")
    logger.info(f"🕒 Eventos recientes cacheados: {len(recent_events)}")

def main():
    redis_client = connect_to_redis()
    if not redis_client:
        return False
    
    events = load_events_from_hdfs()
    if not events:
        logger.error("No se pudieron cargar eventos")
        return False
    
    cache_events_by_criteria(redis_client, events)
    logger.info("Pipeline de caché ejecutado exitosamente")
    return True

if __name__ == "__main__":
    main()

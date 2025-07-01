import os
import sys
import subprocess
import logging
import time
import json
import redis
from redis.exceptions import ConnectionError as RedisConnectionError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

REDIS_HOST = os.environ.get("REDIS_HOST", "cache")
HDFS_RESULTS_BASE_DIR = "/user/hadoop/waze_analysis"
HDFS_RESULTS_PATHS = {
    "stats:commune_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "commune_summary.json"),
    "stats:type_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "type_summary.json"),
    "stats:daily_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "daily_summary.json"),
    "stats:hourly_summary": os.path.join(HDFS_RESULTS_BASE_DIR, "hourly_summary.json")
}

def connect_to_redis():
    for i in range(12):
        try:
            r_client = redis.Redis(host=REDIS_HOST, port=6379, db=0, decode_responses=True)
            r_client.ping()
            return r_client
        except RedisConnectionError as e:
            logger.warning(f"Fallo de conexión a Redis: {e}. Reintentando...")
            time.sleep(5)
    return None

def get_hdfs_data(hdfs_path):
    cat_command = ["hdfs", "dfs", "-cat", f"{hdfs_path}/part-r-*"]
    try:
        result = subprocess.run(cat_command, capture_output=True, text=True, check=False)
        if result.returncode != 0: return []
        return [line for line in result.stdout.strip().split('\n') if line]
    except Exception as e:
        logger.error(f"Error inesperado al leer de HDFS '{hdfs_path}': {e}")
        return []

def main():
    r_client = connect_to_redis()
    if not r_client: sys.exit(1)
    
    pipe = r_client.pipeline()
    for pattern in ["stats:*"]:
        keys_to_delete = list(r_client.scan_iter(pattern))
        if keys_to_delete: pipe.delete(*keys_to_delete)
    
    for key_name, hdfs_path in HDFS_RESULTS_PATHS.items():
        lines = get_hdfs_data(hdfs_path)
        if not lines: continue
        
        logger.info(f"Cargando {len(lines)} registros para '{key_name}'...")
        for line in lines:
            try:
                data = json.loads(line)
                if key_name == "stats:commune_summary":
                    pipe.hset(key_name, data['group::commune'], str(data['total_incidents']))
                elif key_name == "stats:type_summary":
                    pipe.hset(key_name, data['group::standardized_type'], str(data['total_occurrences']))
                elif key_name == "stats:daily_summary":
                    key = f"stats:daily_summary:{data['group::event_date']}:{data['group::standardized_type']}:{data['group::commune']}"
                    pipe.set(key, str(data['incidents_count']))
                elif key_name == "stats:hourly_summary":
                    key = f"stats:hourly_summary:{data['group::event_hour']}:{data['group::standardized_type']}:{data['group::commune']}"
                    pipe.set(key, str(data['incidents_count']))
            except (json.JSONDecodeError, KeyError):
                continue
    
    if len(pipe) > 0:
        pipe.execute()
        logger.info(f"Pipeline de Redis ejecutado.")
    else:
        logger.warning("No se encontraron resultados de Pig para cargar.")

    r_client.close()

if __name__ == "__main__":
    main()
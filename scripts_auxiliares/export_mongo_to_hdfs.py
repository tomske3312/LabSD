import os
import sys
import subprocess
import logging
import time
import pymongo
from pymongo.errors import ConnectionFailure

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s', handlers=[logging.StreamHandler(sys.stdout)])
logger = logging.getLogger(__name__)

MONGO_HOST = os.environ.get("MONGO_HOST", "storage_db")
LOCAL_TSV_PATH = "/tmp/waze_events.tsv"
HDFS_INPUT_DIR = "/user/hadoop/waze_input"
HDFS_TARGET_PATH = os.path.join(HDFS_INPUT_DIR, "waze_events.tsv")
MIN_EVENTS_TO_PROCESS = 100

def connect_to_mongodb():
    for i in range(10):
        try:
            client = pymongo.MongoClient(MONGO_HOST, 27017, serverSelectionTimeoutMS=5000)
            client.admin.command('ping')
            return client
        except ConnectionFailure as err:
            logger.warning(f"Fallo de conexión a MongoDB: {err}. Reintentando en 10s...")
            time.sleep(10)
    return None

def wait_for_data(collection):
    for i in range(60):
        count = collection.count_documents({})
        if count >= MIN_EVENTS_TO_PROCESS:
            logger.info(f"Umbral de {MIN_EVENTS_TO_PROCESS} eventos alcanzado. Exportando {count} documentos.")
            return list(collection.find({}, {'_id': 0}))
        logger.info(f"Esperando más datos... {count}/{MIN_EVENTS_TO_PROCESS}. Próxima comprobación en 10s.")
        time.sleep(10)
    logger.error("No se alcanzó el umbral de eventos para procesar. Abortando.")
    return None

def main():
    mongo_client = connect_to_mongodb()
    if not mongo_client: sys.exit(1)
    
    collection = mongo_client.waze_data.events
    documents = wait_for_data(collection)
    mongo_client.close()

    if documents is None: sys.exit(1)

    expected_fields = ["event_id", "type", "address", "city", "scrape_timestamp"]
    with open(LOCAL_TSV_PATH, "w", encoding="utf-8") as f:
        for doc in documents:
            row = [str(doc.get(field, "")).replace('\t', ' ').replace('\n', ' ') for field in expected_fields]
            f.write("\t".join(row) + "\n")
    
    subprocess.run(["hdfs", "dfs", "-mkdir", "-p", HDFS_INPUT_DIR], check=True)
    subprocess.run(["hdfs", "dfs", "-put", "-f", LOCAL_TSV_PATH, HDFS_TARGET_PATH], check=True)
    logger.info(f"Archivo subido a HDFS: {HDFS_TARGET_PATH}.")

if __name__ == "__main__":
    main()
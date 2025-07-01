import tempfile
import uuid
import time
import json
import sys
import logging
import os
import requests  # AÑADIR ESTA IMPORTACIÓN
from datetime import datetime
from collections import defaultdict

# Selenium imports
try:
    from selenium import webdriver
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.webdriver.common.by import By
    from selenium.common.exceptions import TimeoutException
except ModuleNotFoundError:
    print("ERROR: El módulo 'selenium' no está instalado.")
    sys.exit(1)

# --- Configuración de Logging ---
LOG_FILENAME = "data/LOG.txt"
os.makedirs(os.path.dirname(LOG_FILENAME), exist_ok=True)

log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler_file = logging.FileHandler(LOG_FILENAME, encoding='utf-8')
log_handler_file.setFormatter(log_formatter)
log_handler_console = logging.StreamHandler(sys.stdout)
log_handler_console.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.INFO)
if not logger.handlers:
    logger.addHandler(log_handler_file)
    logger.addHandler(log_handler_console)

# --- Configuración del Área de Santiago ---
TARGET_AREA = {
    "lat_max": -33.3503,  # Norte
    "lat_min": -33.6106,  # Sur
    "lon_min": -70.7778,  # Oeste
    "lon_max": -70.4990   # Este
}

# --- Almacenamiento ---
scraped_events = []
event_id_counts = defaultdict(int)
output_filename = "data/waze_events.json"

def save_events():
    """Guarda eventos en JSON."""
    try:
        os.makedirs(os.path.dirname(output_filename), exist_ok=True)
        with open(output_filename, 'w', encoding='utf-8') as f:
            json.dump(scraped_events, f, indent=4, ensure_ascii=False)
        logger.info(f"Eventos guardados: {len(scraped_events)}")
    except Exception as e:
        logger.error(f"Error guardando eventos: {e}")

def load_events():
    """Carga eventos previos."""
    global scraped_events, event_id_counts
    try:
        if os.path.exists(output_filename):
            with open(output_filename, "r", encoding="utf-8") as f:
                scraped_events = json.load(f)
                event_id_counts.clear()
                for event in scraped_events:
                    event_id = event.get('event_id')
                    if event_id:
                        event_id_counts[event_id] += 1
                logger.info(f"Cargados {len(scraped_events)} eventos previos")
        else:
            scraped_events = []
            event_id_counts.clear()
    except Exception as e:
        logger.error(f"Error cargando eventos: {e}")
        scraped_events = []
        event_id_counts.clear()

def get_georss_data(driver, lat_max, lat_min, lon_min, lon_max):
    """Obtiene datos del endpoint georss de Waze."""
    georss_url = f"https://www.waze.com/live-map/api/georss?top={lat_max}&bottom={lat_min}&left={lon_min}&right={lon_max}&env=row&types=alerts,traffic,users"
    
    logger.info(f"Accediendo a: {georss_url}")
    
    try:
        driver.get(georss_url)
        time.sleep(2)  # Esperar que cargue
        
        # Buscar el elemento <pre> que contiene el JSON
        try:
            pre_element = WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.TAG_NAME, "pre"))
            )
            json_text = pre_element.text
            
            if json_text.strip():
                data = json.loads(json_text)
                return data
            else:
                logger.warning("Elemento <pre> encontrado pero vacío")
                return None
                
        except TimeoutException:
            logger.warning("No se encontró elemento <pre> con JSON")
            return None
            
    except Exception as e:
        logger.error(f"Error obteniendo datos georss: {e}")
        return None

def process_alerts(alerts_data):
    """Procesa las alertas del JSON de georss."""
    if not alerts_data or 'alerts' not in alerts_data:
        logger.info("No hay alertas en los datos")
        return 0
    
    new_events = 0
    alerts = alerts_data['alerts']
    logger.info(f"Procesando {len(alerts)} alertas")
    
    for alert in alerts:
        try:
            # Extraer información del alert
            alert_type = alert.get('type', 'Desconocido')
            location = alert.get('location', {})
            lat = location.get('y', 0)
            lon = location.get('x', 0)
            
            # NUEVO: Obtener dirección real de la calle
            logger.info(f"Obteniendo dirección para coordenadas {lat:.4f}, {lon:.4f}")
            street_address = get_street_address(lat, lon)
            
            # Información adicional
            report_time = alert.get('pubMillis', int(time.time() * 1000))
            report_time_str = datetime.fromtimestamp(report_time / 1000).strftime('%Y-%m-%d %H:%M:%S')
            
            # Crear ID único
            event_id = f"{alert_type}-{lat:.4f}-{lon:.4f}-{report_time}"
            
            # Verificar si ya existe
            if event_id_counts[event_id] == 0:
                event_data = {
                    "event_id": event_id,
                    "type": alert_type,
                    "address": street_address,  # AHORA ES LA DIRECCIÓN REAL
                    "latitude": lat,
                    "longitude": lon,
                    "report_time": report_time_str,
                    "reporter": alert.get('reportBy', 'Desconocido'),
                    "confidence": alert.get('confidence', 0),
                    "scrape_timestamp": datetime.utcnow().isoformat()
                }
                
                scraped_events.append(event_data)
                event_id_counts[event_id] += 1
                new_events += 1
                
                logger.info(f"NUEVO EVENTO: {alert_type} en {street_address}")
            
            # Pausa breve para no sobrecargar la API de geocodificación
            time.sleep(0.5)
        
        except Exception as e:
            logger.error(f"Error procesando alerta individual: {e}")
            continue
    
    return new_events


def create_grid(area, grid_size=0.02):
    """Crea una cuadrícula de coordenadas para cubrir el área."""
    grid_points = []
    
    lat = area["lat_min"]
    while lat <= area["lat_max"]:
        lon = area["lon_min"]
        while lon <= area["lon_max"]:
            grid_points.append({
                "lat_max": min(lat + grid_size, area["lat_max"]),
                "lat_min": lat,
                "lon_min": lon,
                "lon_max": min(lon + grid_size, area["lon_max"])
            })
            lon += grid_size
        lat += grid_size
    
    return grid_points

def get_street_address(lat, lon):
    """Obtiene la dirección de la calle usando geocodificación inversa."""
    try:
        # API de Nominatim (OpenStreetMap) - Gratuita
        url = f"https://nominatim.openstreetmap.org/reverse?format=json&lat={lat}&lon={lon}&addressdetails=1"
        
        headers = {
            'User-Agent': 'WazeScraper/1.0 (Contact: admin@example.com)'  # Requerido por Nominatim
        }
        
        response = requests.get(url, headers=headers, timeout=5)
        
        if response.status_code == 200:
            data = response.json()
            
            # Extraer la dirección más relevante
            address_parts = data.get('address', {})
            
            # Intentar obtener calle y número
            street_number = address_parts.get('house_number', '')
            street_name = address_parts.get('road', '')
            
            if street_name:
                if street_number:
                    full_address = f"{street_name} {street_number}"
                else:
                    full_address = street_name
                
                # Añadir comuna/barrio si está disponible
                suburb = address_parts.get('suburb', '') or address_parts.get('neighbourhood', '')
                if suburb:
                    full_address += f", {suburb}"
                
                return full_address
            else:
                # Si no hay calle, usar el display_name completo pero más corto
                display_name = data.get('display_name', '')
                if display_name:
                    # Tomar solo las primeras 2-3 partes de la dirección
                    parts = display_name.split(',')[:3]
                    return ', '.join(parts).strip()
        
        # Si falla todo, devolver coordenadas
        return f"Lat: {lat:.4f}, Lon: {lon:.4f}"
        
    except Exception as e:
        logger.warning(f"Error obteniendo dirección para {lat}, {lon}: {e}")
        return f"Lat: {lat:.4f}, Lon: {lon:.4f}"

def get_driver():
    """Inicializa el driver de Chrome."""
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")  # ACTIVAMOS headless para Docker
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=es-419")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument("--log-level=3")
    chrome_options.add_argument("--disable-web-security")
    chrome_options.add_argument("--disable-features=VizDisplayCompositor")
    chrome_options.add_argument("--disable-background-timer-throttling")
    chrome_options.add_argument("--disable-backgrounding-occluded-windows")
    chrome_options.add_argument("--disable-renderer-backgrounding")
    chrome_options.add_argument("--disable-ipc-flooding-protection")
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])
    
    logger.info("Configuración de Chrome lista (modo headless)")
    return webdriver.Chrome(options=chrome_options)

# --- Flujo Principal ---
if __name__ == "__main__":
    driver = None
    
    try:
        logger.info("=== INICIANDO SCRAPER WAZE OPTIMIZADO ===")
        
        # Cargar eventos previos
        load_events()
        
        # Inicializar WebDriver
        logger.info("Inicializando Chrome WebDriver...")
        driver = get_driver()
        logger.info("WebDriver inicializado correctamente")
        
        # BUCLE INFINITO - El scraper correrá indefinidamente
        cycle_count = 0
        while True:
            cycle_count += 1
            logger.info(f"\n{'='*20} CICLO {cycle_count} {'='*20}")
            
            # Crear cuadrícula de áreas a consultar
            grid_points = create_grid(TARGET_AREA)
            logger.info(f"Cuadrícula creada: {len(grid_points)} puntos")
            
            total_new_events = 0
            
            # Procesar cada punto de la cuadrícula
            for i, grid_point in enumerate(grid_points, 1):
                logger.info(f"\n--- Ciclo {cycle_count} - Punto {i}/{len(grid_points)} ---")
                
                # Obtener datos del georss para esta área
                data = get_georss_data(
                    driver,
                    grid_point["lat_max"],
                    grid_point["lat_min"], 
                    grid_point["lon_min"],
                    grid_point["lon_max"]
                )
                
                if data:
                    # Procesar alertas
                    new_events = process_alerts(data)
                    total_new_events += new_events
                    
                    # Guardar después de cada punto si hay eventos nuevos
                    if new_events > 0:
                        save_events()
                
                # Pausa entre requests para no sobrecargar el servidor
                time.sleep(1)
            
            logger.info(f"\n=== CICLO {cycle_count} COMPLETADO ===")
            logger.info(f"Nuevos eventos encontrados: {total_new_events}")
            logger.info(f"Total eventos en archivo: {len(scraped_events)}")
            
            # Guardar resultado final del ciclo
            save_events()
            
            # Pausa entre ciclos completos (15 minutos)
            logger.info(f"Esperando 15 minutos antes del siguiente ciclo...")
            time.sleep(900)  # 15 minutos = 900 segundos
        
    except KeyboardInterrupt:
        logger.warning("Interrupción por teclado detectada")
    except Exception as e:
        logger.error(f"Error crítico: {e}")
    finally:
        # Limpieza
        if driver:
            logger.info("Cerrando navegador...")
            driver.quit()
        logger.info("Script finalizado")
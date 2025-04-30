# -*- coding: utf-8 -*-

import time
import re
import json # Para guardar
import sys # Para salir en caso de error grave
import traceback # Para imprimir detalles de error
import random # Para el inicio aleatorio
import logging # Para guardar logs en archivo
from datetime import datetime # Para logging timestamp
import os # Para asegurar que el directorio de logs existe
from collections import defaultdict # Para contar event_id

# Intenta importar Selenium, sal si no está instalado
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    # from selenium.webdriver.chrome.service import Service as ChromeService # No necesario si chromedriver está en PATH
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import NoSuchElementException, TimeoutException, StaleElementReferenceException, ElementClickInterceptedException, ElementNotInteractableException
    from selenium.webdriver.common.action_chains import ActionChains
except ModuleNotFoundError:
    # Usar print aquí porque el logger aún no está configurado
    print("="*60)
    print("ERROR: El módulo 'selenium' no está instalado.")
    print("Por favor, instálalo: pip install selenium")
    print("="*60)
    sys.exit(1)

# Ya no se necesitan dateutil ni pytz

# --- Configuración de Logging ---
LOG_FILENAME = "LOG.txt" # Este archivo se creará dentro del contenedor
log_formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
log_handler_file = logging.FileHandler(LOG_FILENAME, encoding='utf-8')
log_handler_file.setFormatter(log_formatter)
log_handler_console = logging.StreamHandler(sys.stdout)
log_handler_console.setFormatter(log_formatter)

logger = logging.getLogger()
logger.setLevel(logging.WARNING) # Nivel de Log a WARNING para reducir output
if not logger.handlers:
    logger.addHandler(log_handler_file)
    logger.addHandler(log_handler_console)

# --- Configuración del Script ---
WAZE_URL = "https://www.waze.com/es-419/live-map/"
TARGET_AREA = {
    "lat_max": -33.3503, # Norte
    "lat_min": -33.6106, # Sur
    "lon_min": -70.7778, # Oeste
    "lon_max": -70.4990  # Este
}
TARGET_ZOOM = 16
logger.info(f"Usando nivel de zoom objetivo: {TARGET_ZOOM}")
CLICKS_FOR_TARGET_ZOOM = 2

# --- Límite máximo de eventos a recolectar ---
MAX_EVENTS = 10000 # Límite de eventos *guardados* (considerando el límite de repetición)
logger.info(f"Objetivo informativo de eventos a recolectar: {MAX_EVENTS}") # Cambiado a informativo

# --- Configuración Anti-Atasco ---
STUCK_THRESHOLD = 5 # Número de vistas SIN eventos nuevos antes de saltar
logger.info(f"Umbral anti-atasco (vistas sin eventos nuevos): {STUCK_THRESHOLD}")

# --- Pausa entre barridos completos (en segundos) ---
SLEEP_BETWEEN_SWEEPS = 300 # 5 minutos de pausa
logger.info(f"Pausa entre barridos completos: {SLEEP_BETWEEN_SWEEPS} segundos")


# Selectores (sin cambios)
MAP_SELECTOR = "#map"
MARKER_SELECTOR = "div.leaflet-marker-icon.wm-alert-icon.leaflet-interactive, div.leaflet-marker-icon.wm-alert-cluster-icon.leaflet-interactive"
LOCATION_SELECTOR = "div.wm-attribution-control__latlng > span"
POPUP_DETAILS_SELECTOR = "div.wm-alert-details"
TYPE_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > h4"
ADDRESS_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__address"
# DESCRIPTION_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__descriptions > p" # No usado
DATE_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__reporter > div > div.wm-alert-details__time"
REPORTER_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__reporter > div > div.wm-alert-details__reporter-name > b"
CLOSE_POPUP_SELECTOR = "#map div.leaflet-popup-pane > div > a"
logger.info(f"Usando selector para cerrar popup de evento: {CLOSE_POPUP_SELECTOR}")
OVERLAY_SELECTOR = "div.waze-tour-step__overlay"
OVERLAY_CLOSE_BUTTON_SELECTOR = "button.waze-tour-step__close-button" # <-- ¡¡¡AJUSTA ESTE SELECTOR SI LO ENCUENTRAS!!!
logger.info(f"Usando selector para CERRAR overlay de tour: {OVERLAY_CLOSE_BUTTON_SELECTOR}")
INITIAL_TOOLTIP_BUTTON_SELECTOR = "body > div:nth-child(13) > div.waze-tooltip.waze-tour-tooltip__root > button"
logger.info(f"Usando selector para botón 'Entendido' inicial: {INITIAL_TOOLTIP_BUTTON_SELECTOR}")
FILTERCLOSEPART1 = "#map > div.wm-cards.is-destination > div.wm-card.is-routing > div > div.wm-routing__title > button"
FILTER2_HOST_SELECTOR = "div.wz-popup-overlay.wz-sidebar-overlay.wz-sidebar-open ul:nth-child(2) li:nth-child(3) wz-toggle-switch"
FILTER2_SHADOW_TARGET_SELECTOR = "span > label > span"
FILTERCLOSEPART3 = "body > div.wz-popup-overlay.wz-sidebar-overlay.wz-sidebar-open > div > div > button"
logger.info(f"Usando selector de filtro 1: {FILTERCLOSEPART1}")
logger.info(f"Usando selector HOST para filtro 2 (Shadow DOM): {FILTER2_HOST_SELECTOR}")
logger.info(f"Usando selector TARGET para filtro 2 (Shadow DOM): {FILTER2_SHADOW_TARGET_SELECTOR}")
logger.info(f"Usando selector de filtro 3: {FILTERCLOSEPART3}")
AD_BANNER_CLOSE_SELECTOR = "#root > div.wz-downloadbar > button"
logger.info(f"Usando selector para cerrar banner de publicidad: {AD_BANNER_CLOSE_SELECTOR}")
ZOOM_IN_BUTTON_SELECTOR = "a.leaflet-control-zoom-in"
logger.info(f"Usando selector para botón Zoom In: {ZOOM_IN_BUTTON_SELECTOR}")
COOKIE_BANNER_SELECTOR = "#onetrust-banner-sdk"
COOKIE_ACCEPT_SELECTOR = "#onetrust-accept-btn-handler"

# --- Almacenamiento ---
scraped_events = []
event_id_counts = defaultdict(int) # Contador para limitar repeticiones por ID
output_filename = "waze_events.json" # Se guardará en /app dentro del contenedor

# --- Funciones Auxiliares ---

def dismiss_initial_elements(driver, wait_time=3):
    """Intenta detectar y cerrar elementos que aparecen UNA VEZ al inicio."""
    logger.info("\n--- Intentando cerrar elementos iniciales (Ads, Tooltips, Cookies, Filtros)... ---")
    initial_dismissed_count = 0
    short_wait = WebDriverWait(driver, wait_time)

    # 1. Barra de publicidad
    try:
        ad_close_button = short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, AD_BANNER_CLOSE_SELECTOR)))
        logger.info("  [Initial] Detectada barra de publicidad inicial. Intentando cerrar...")
        try: ad_close_button.click()
        except Exception: driver.execute_script("arguments[0].click();", ad_close_button)
        logger.info("  [Initial] Barra de publicidad cerrada.")
        time.sleep(0.3); initial_dismissed_count += 1
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al intentar cerrar barra de ad: {e}")

    # 2. Tooltip inicial "Entendido"
    try:
        entendido_button = short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, INITIAL_TOOLTIP_BUTTON_SELECTOR)))
        logger.info("  [Initial] Detectado tooltip inicial 'Entendido'. Intentando clic...")
        try: entendido_button.click()
        except Exception: driver.execute_script("arguments[0].click();", entendido_button)
        logger.info("  [Initial] Botón 'Entendido' clickeado.")
        time.sleep(0.5); initial_dismissed_count += 1
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al intentar cerrar tooltip inicial 'Entendido': {e}")

    # 3. Banner de cookies
    try:
        cookie_wait = WebDriverWait(driver, 1.5)
        cookie_banner = cookie_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, COOKIE_BANNER_SELECTOR)))
        if cookie_banner.is_displayed():
            logger.info("  [Initial] Detectado banner de cookies. Intentando aceptar...")
            accept_button = WebDriverWait(driver, 3).until(EC.element_to_be_clickable((By.CSS_SELECTOR, COOKIE_ACCEPT_SELECTOR)))
            accept_button.click()
            logger.info("  [Initial] Banner de cookies aceptado.")
            time.sleep(0.5); initial_dismissed_count += 1
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al intentar cerrar banner de cookies: {e}")

    # 4. Filtro 1
    try:
        filter1_button = short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, FILTERCLOSEPART1)))
        logger.info(f"  [Initial] Detectado elemento de filtro 1 ({FILTERCLOSEPART1}). Intentando clic...")
        try: filter1_button.click()
        except Exception: driver.execute_script("arguments[0].click();", filter1_button)
        logger.info("  [Initial] Elemento de filtro 1 clickeado.")
        time.sleep(0.3); initial_dismissed_count += 1
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al intentar clickear filtro 1: {e}")

    # 5. Filtro 2 (Shadow DOM)
    try:
        filter2_host = short_wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, FILTER2_HOST_SELECTOR)))
        logger.info(f"  [Initial] Detectado HOST de filtro 2 ({FILTER2_HOST_SELECTOR}). Intentando interactuar con Shadow DOM...")
        click_script = f"""
            const host = arguments[0];
            const targetElement = host.shadowRoot.querySelector('{FILTER2_SHADOW_TARGET_SELECTOR}');
            if (targetElement) {{ targetElement.click(); return true; }} else {{ return false; }}
        """
        try:
            clicked = driver.execute_script(click_script, filter2_host)
            if clicked:
                logger.info(f"  [Initial] Elemento TARGET de filtro 2 ({FILTER2_SHADOW_TARGET_SELECTOR}) clickeado vía JS.")
                time.sleep(0.3); initial_dismissed_count += 1
            else:
                logger.warning(f"  [Initial] ADVERTENCIA: No se encontró el elemento TARGET ('{FILTER2_SHADOW_TARGET_SELECTOR}') dentro del Shadow DOM.")
        except Exception as js_click_err: logger.error(f"  [Initial] Error al ejecutar JS para clickear filtro 2: {js_click_err}")
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al buscar o interactuar con filtro 2 (Shadow DOM): {e}")

    # 6. Filtro 3
    try:
        filter3_button = short_wait.until(EC.element_to_be_clickable((By.CSS_SELECTOR, FILTERCLOSEPART3)))
        logger.info(f"  [Initial] Detectado elemento de filtro 3 ({FILTERCLOSEPART3}). Intentando clic...")
        try: filter3_button.click()
        except Exception: driver.execute_script("arguments[0].click();", filter3_button)
        logger.info("  [Initial] Elemento de filtro 3 clickeado.")
        time.sleep(0.3); initial_dismissed_count += 1
    except (NoSuchElementException, TimeoutException): pass
    except Exception as e: logger.error(f"  [Initial] Error al intentar clickear filtro 3: {e}")

    logger.info(f"--- Cierre de elementos iniciales finalizado ({initial_dismissed_count} elementos gestionados) ---")


def dismiss_recurring_overlays(driver, wait_time=0.5):
    """Intenta detectar y cerrar overlays que pueden REAPARECER (ej: Tour)."""
    overlay_dismissed = False
    short_wait = WebDriverWait(driver, wait_time)

    if OVERLAY_CLOSE_BUTTON_SELECTOR != "button.waze-tour-step__close-button":
        try:
            overlays = short_wait.until(lambda d: d.find_elements(By.CSS_SELECTOR, OVERLAY_SELECTOR))
            overlay_visible = any(o.is_displayed() for o in overlays)
            if overlay_visible:
                logger.info("  [Overlay] Detectado overlay de tour/ayuda recurrente. Intentando cerrar...")
                try:
                    close_button = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, OVERLAY_CLOSE_BUTTON_SELECTOR)))
                    try: close_button.click()
                    except Exception: driver.execute_script("arguments[0].click();", close_button)
                    logger.info("  [Overlay] Botón de cierre del overlay clickeado.")
                    time.sleep(0.3); overlay_dismissed = True
                except (NoSuchElementException, TimeoutException):
                     logger.warning(f"  [Overlay] ADVERTENCIA: No se encontró/clickeable botón cierre overlay tour ('{OVERLAY_CLOSE_BUTTON_SELECTOR}').")
                except Exception as click_err: logger.error(f"  [Overlay] Error al clickear botón cierre overlay tour: {click_err}")
        except (NoSuchElementException, TimeoutException): pass
        except Exception as e: logger.error(f"  [Overlay] Error buscando overlay de tour recurrente: {e}")

    return overlay_dismissed


def set_initial_zoom(driver, wait, clicks_needed):
    """Intenta hacer clic en el botón de zoom '+' varias veces."""
    logger.info(f"\n--- Intentando ajustar zoom inicial con {clicks_needed} clics ---")
    zoom_success = True
    try:
        zoom_in_button_locator = (By.CSS_SELECTOR, ZOOM_IN_BUTTON_SELECTOR)
        dismiss_recurring_overlays(driver, wait_time=1)
        wait.until(EC.element_to_be_clickable(zoom_in_button_locator))
        for i in range(clicks_needed):
            logger.info(f"  [Zoom] Realizando clic de zoom #{i+1}...")
            try:
                zoom_in_button = wait.until(EC.element_to_be_clickable(zoom_in_button_locator))
                zoom_in_button.click()
                time.sleep(1.0)
            except Exception as zoom_click_err:
                logger.warning(f"  [Zoom] Error al hacer clic en zoom ({zoom_click_err}), intentando con JS...")
                try:
                    zoom_in_button = wait.until(EC.presence_of_element_located(zoom_in_button_locator))
                    driver.execute_script("arguments[0].click();", zoom_in_button)
                    time.sleep(1.0)
                except Exception as js_zoom_click_err:
                    logger.error(f"  [Zoom] Error fatal al hacer clic en zoom con JS: {js_zoom_click_err}.")
                    zoom_success = False; break
        if zoom_success: logger.info(f"--- Ajuste de zoom inicial completado (o intentado) ---")
        else: logger.warning(f"--- Ajuste de zoom inicial fallido ---")
        return zoom_success
    except (NoSuchElementException, TimeoutException):
        logger.error("[Zoom] Error: No se encontró el botón de zoom in o no fue clickeable.")
        return False
    except Exception as e:
        logger.exception(f"[Zoom] Error inesperado durante el ajuste de zoom: {e}")
        return False


def get_current_location(driver, wait, retries=2, delay=0.4):
    """Obtiene lat/lon actual, con reintentos si el valor no cambia."""
    last_location_text = None
    for attempt in range(retries + 1):
        try:
            location_wait = WebDriverWait(driver, 3)
            location_element = location_wait.until(EC.visibility_of_element_located((By.CSS_SELECTOR, LOCATION_SELECTOR)))
            current_location_text = location_element.text

            if attempt > 0 and current_location_text == last_location_text:
                logger.debug(f"  [Location] Intento {attempt+1}: Texto de ubicación no cambió ('{current_location_text}'), esperando {delay}s...")
                time.sleep(delay)
                continue

            last_location_text = current_location_text
            match = re.search(r"(-?\d{1,2}\.\d+)\s*[,|]?\s*(-?\d{1,3}\.\d+)", current_location_text)
            if match:
                lat = float(match.group(1))
                lon = float(match.group(2))
                logger.debug(f"  [Location] Ubicación obtenida: Lat={lat:.4f}, Lon={lon:.4f}")
                return lat, lon
            else:
                logger.warning(f"Advertencia: No se pudo extraer lat/lon del texto: '{current_location_text}'")
                return None, None

        except TimeoutException:
            logger.warning(f"Advertencia: Timeout esperando el elemento de ubicación (Intento {attempt+1}/{retries+1}).")
            if attempt < retries: time.sleep(delay)
            else: return None, None
        except Exception as e:
            logger.exception(f"Error inesperado al obtener ubicación: {e}")
            return None, None
    return None, None


def pan_map(driver, wait, direction_key, steps=1):
    """Envía teclas de flecha al mapa para moverlo e informa, usando ActionChains."""
    map_element = None
    try:
        dismiss_recurring_overlays(driver, wait_time=0.5)
        map_element = driver.find_element(By.CSS_SELECTOR, MAP_SELECTOR)

        actions = ActionChains(driver)
        actions.move_to_element(map_element).click()
        for _ in range(steps):
            actions.send_keys(direction_key)
            actions.pause(0.2)

        logger.debug(f"  [Pan] Ejecutando movimiento: {str(direction_key).split('.')[-1]} x {steps}")
        actions.perform()
        time.sleep(2.0)

        lat, lon = get_current_location(driver, wait, retries=2, delay=0.6)
        if lat is not None:
             logger.debug(f"  [Pan] Nueva ubicación post-movimiento: Lat={lat:.4f}, Lon={lon:.4f}")
        else:
             logger.warning("  [Pan] No se pudo obtener ubicación después del movimiento.")
        return lat, lon

    except ElementNotInteractableException:
        logger.warning("  [Pan] Error: Mapa no interactuable. Intentando cerrar overlays y reintentar UNA VEZ...")
        if dismiss_recurring_overlays(driver, wait_time=0.5):
            time.sleep(0.3)
            try:
                map_element = driver.find_element(By.CSS_SELECTOR, MAP_SELECTOR)
                actions = ActionChains(driver)
                actions.move_to_element(map_element).click()
                for _ in range(steps):
                    actions.send_keys(direction_key)
                    actions.pause(0.2)
                logger.debug(f"  [Pan] Ejecutando REINTENTO de movimiento: {str(direction_key).split('.')[-1]} x {steps}")
                actions.perform()
                time.sleep(2.0)
                lat, lon = get_current_location(driver, wait, retries=2, delay=0.6)
                if lat is not None:
                    logger.debug(f"  [Pan] Nueva ubicación post-reintento: Lat={lat:.4f}, Lon={lon:.4f}")
                else:
                    logger.warning("  [Pan] No se pudo obtener ubicación después del reintento.")
                return lat, lon
            except Exception as retry_err:
                logger.error(f"  [Pan] Reintento de movimiento falló: {retry_err}.")
                return None, None
        else:
            logger.warning("  [Pan] No se pudieron cerrar overlays durante el reintento.")
            return None, None

    except (NoSuchElementException, StaleElementReferenceException) as e:
        logger.error(f"  [Pan] Error localizando el mapa: {e}")
    except Exception as e:
        logger.exception(f"  [Pan] Error inesperado al mover mapa: {e}")

    return None, None


def close_popup(driver, wait):
    """Intenta cerrar el popup de detalles usando el selector específico."""
    popup_closed = False
    if CLOSE_POPUP_SELECTOR:
        try:
            close_button = WebDriverWait(driver, 2).until(EC.element_to_be_clickable((By.CSS_SELECTOR, CLOSE_POPUP_SELECTOR)))
            try: close_button.click()
            except Exception: driver.execute_script("arguments[0].click();", close_button)
            popup_closed = True; time.sleep(0.3)
        except TimeoutException: pass
        except Exception as e: logger.error(f"  -> Error al intentar cerrar popup: {e}")

    dismiss_recurring_overlays(driver, wait_time=0.5)
    return popup_closed

def try_click(driver, wait, element):
    """Intenta hacer clic en un elemento, manejando intercepciones."""
    try:
        clickable_element = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(element))
        clickable_element.click()
        return True
    except ElementClickInterceptedException as eci:
        error_msg = str(eci).splitlines()[0]
        logger.warning(f"  -> Clic interceptado! ({error_msg})")
        if dismiss_recurring_overlays(driver, wait_time=0.5):
            time.sleep(0.3)
            try:
                clickable_element = WebDriverWait(driver, 3).until(EC.element_to_be_clickable(element))
                clickable_element.click()
                return True
            except Exception as retry_err:
                logger.warning(f"  -> Reintento de clic normal falló: {retry_err}.")
        logger.info("  -> Usando clic con JavaScript como último recurso...")
        try:
            driver.execute_script("arguments[0].click();", element)
            return True
        except Exception as js_err:
            logger.error(f"  -> Clic con JavaScript también falló: {js_err}")
            return False
    except (TimeoutException, StaleElementReferenceException) as e:
         logger.warning(f"  -> Error esperando clickeabilidad o elemento obsoleto: {e}")
         return False
    except Exception as e:
        logger.exception(f"  -> Error inesperado durante el clic: {e}")
        return False


def scrape_event_details(driver, wait, event_element):
    """
    Hace clic en un evento, extrae detalles, genera event_id (reporter+address+type),
    verifica límite de repetición y guarda si procede. Cierra popup.
    """
    global scraped_events, event_id_counts # Ya no necesitamos modificar keep_running aquí
    event_data = {}
    event_was_added = False

    short_wait = WebDriverWait(driver, 5)
    if not try_click(driver, short_wait, event_element):
        logger.warning("  -> Falló el clic en el evento. Saltando.")
        return False

    try:
        details_popup = WebDriverWait(driver, 8).until(EC.visibility_of_element_located((By.CSS_SELECTOR, POPUP_DETAILS_SELECTOR)))

        details_text = []
        event_type = "Desconocido"
        event_address = "No disponible"
        event_date_str = "No disponible"
        reporter_name = "Desconocido"

        try: event_type = details_popup.find_element(By.CSS_SELECTOR, TYPE_SELECTOR).text; details_text.append(f"T:{event_type}")
        except NoSuchElementException: pass
        try: event_address = details_popup.find_element(By.CSS_SELECTOR, ADDRESS_SELECTOR).text; details_text.append(f"D:{event_address}")
        except NoSuchElementException: pass
        try:
            event_date_str = details_popup.find_element(By.CSS_SELECTOR, DATE_SELECTOR).text
            details_text.append(f"H:{event_date_str}")
        except NoSuchElementException: pass
        try:
            reporter_name = details_popup.find_element(By.CSS_SELECTOR, REPORTER_SELECTOR).text; details_text.append(f"R:{reporter_name}")
        except NoSuchElementException: pass


        event_data['type'] = event_type
        event_data['address'] = event_address
        event_data['timestamp_original_relative'] = event_date_str
        event_data['reporter'] = reporter_name

        event_id = f"{reporter_name or 'Desconocido'}-{event_address or 'No disponible'}-{event_type or 'Desconocido'}"
        event_data['event_id'] = event_id

        log_summary = " | ".join(filter(None, details_text))
        # Loguear solo si el nivel es INFO o DEBUG
        # if logger.level <= logging.INFO:
        #     logger.info("     " + log_summary + f" | ID: {event_id}")

        # Lógica de conteo y límite de duplicados
        current_count = event_id_counts.get(event_id, 0)

        if current_count < 5:
            scraped_events.append(event_data)
            event_id_counts[event_id] = current_count + 1
            event_was_added = True
            # --- CAMBIO: Usar logger.warning en lugar de print ---
            current_saved_count = len(scraped_events)
            # Usamos warning para que se muestre incluso si el nivel general es WARNING
            logger.warning(f"--- Evento Guardado #{current_saved_count} ---")
            # Formatear el JSON para el log (una sola línea es mejor para logs)
            event_json_string = json.dumps(event_data, ensure_ascii=False)
            logger.warning(f"Datos: {event_json_string}")
            logger.warning("-" * 30) # Separador en el log
            # -----------------------------------------------------
            save_events() # Guardar después de añadir y loguear

            # Loguear progreso cada 100 eventos guardados (esto ya usa logger.warning)
            if current_saved_count % 100 == 0:
                 logger.warning(f"--- Progreso: {current_saved_count} eventos guardados. ({len(event_id_counts)} IDs únicos encontrados) ---")

            # Ya no comprobamos MAX_EVENTS aquí
        else:
            # Loguear si se omite por límite de repetición (solo si el nivel es INFO o DEBUG)
            if logger.level <= logging.INFO:
                 logger.info(f"  -> Evento con ID '{event_id}' omitido (límite de {current_count}/5 repeticiones alcanzado).")
            event_was_added = False

        close_popup(driver, wait)

    except TimeoutException: logger.warning("  -> Error: Timeout esperando el popup de detalles después del clic.")
    except StaleElementReferenceException: logger.warning("  -> Error: El popup o sus elementos se volvieron obsoletos.")
    except Exception as e: logger.exception(f"  -> Error inesperado extrayendo detalles o cerrando popup: {e}")

    return event_was_added


def click_cluster(driver, wait, cluster_element):
    """Hace clic en un cluster e informa. NO fuerza re-escaneo."""
    short_wait = WebDriverWait(driver, 5)
    if not try_click(driver, short_wait, cluster_element):
        logger.warning("  -> Falló el clic en el cluster. Saltando.")
        return False

    logger.info("  -> Clic en cluster realizado.")
    time.sleep(1.0)
    return True

def save_events():
     """Guarda la lista actual de eventos en el archivo JSON."""
     try:
         with open(output_filename, "w", encoding="utf-8") as f:
             json.dump(scraped_events, f, ensure_ascii=False, indent=4)
     except Exception as e:
         logger.exception(f"  -> Error al guardar eventos en JSON: {e}")

def load_events():
    """Carga eventos previamente guardados y reconstruye los contadores de event_id."""
    global scraped_events, event_id_counts
    try:
        if os.path.exists(output_filename):
            with open(output_filename, "r", encoding="utf-8") as f:
                loaded_data = json.load(f)
                count_loaded_events = 0
                event_id_counts.clear()
                for event in loaded_data:
                    event_id = event.get('event_id')
                    if event_id:
                        event_id_counts[event_id] += 1
                        count_loaded_events += 1

                scraped_events = loaded_data
                unique_ids_loaded = len(event_id_counts)
                logger.warning(f"Cargados {count_loaded_events} eventos previos ({unique_ids_loaded} IDs únicos) desde {output_filename}.")

        else:
            logger.warning(f"Archivo {output_filename} no encontrado, empezando de cero.")
            scraped_events = []
            event_id_counts.clear()
    except json.JSONDecodeError:
        logger.error(f"Error: El archivo {output_filename} está corrupto o vacío. Empezando de cero.")
        scraped_events = []
        event_id_counts.clear()
    except Exception as e:
        logger.exception(f"Error inesperado al cargar eventos previos: {e}")
        scraped_events = []
        event_id_counts.clear()


def move_to_coordinate(driver, wait, target_lat, target_lon, max_steps=150):
    """Intenta mover el mapa hacia una coordenada objetivo usando teclas de flecha."""
    logger.info(f"\n--- Intentando mover mapa a Lat={target_lat:.4f}, Lon={target_lon:.4f} ---")
    current_lat, current_lon = get_current_location(driver, wait, retries=3)
    if current_lat is None:
        logger.error("Error: No se pudo obtener la ubicación actual para iniciar el movimiento.")
        return None, None

    steps_taken = 0
    TOLERANCE = 0.008

    while steps_taken < max_steps:
        time.sleep(0.1)
        dismiss_recurring_overlays(driver, wait_time=0.5)
        lat_diff = target_lat - current_lat
        lon_diff = target_lon - current_lon

        if abs(lat_diff) < TOLERANCE and abs(lon_diff) < TOLERANCE:
            logger.info(f"--- Ubicación objetivo alcanzada (aprox.) después de {steps_taken} pasos ---")
            return current_lat, current_lon

        moved_v = False
        moved_h = False
        if abs(lat_diff) >= TOLERANCE:
            direction = Keys.ARROW_UP if lat_diff > 0 else Keys.ARROW_DOWN
            move_result = pan_map(driver, wait, direction, 1)
            if move_result is not None:
                current_lat, current_lon = move_result
                moved_v = True
            else:
                logger.warning("  [MoveDebug] Fallo en movimiento vertical, deteniendo move_to_coordinate.")
                break

        if abs(lon_diff) >= TOLERANCE:
            direction = Keys.ARROW_LEFT if lon_diff < 0 else Keys.ARROW_RIGHT
            move_result_h = pan_map(driver, wait, direction, 1)
            if move_result_h is not None:
                new_lat_h, new_lon_h = move_result_h
                current_lon = new_lon_h
                if not moved_v and new_lat_h is not None:
                    current_lat = new_lat_h
                moved_h = True
            else:
                logger.warning("  [MoveDebug] Fallo en movimiento horizontal, deteniendo move_to_coordinate.")
                break

        steps_taken += 1
        if not moved_v and not moved_h:
             logger.error("Error: No se pudo realizar movimiento en pan_map (ambas direcciones).")
             break

    logger.info(f"--- Movimiento a coordenada detenido (máx pasos o error). Posición final aprox: Lat={current_lat:.4f}, Lon={current_lon:.4f} ---")
    return current_lat, current_lon

# --- Inicialización de WebDriver ---
driver = None
try:
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--headless")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--lang=es-419")
    chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
    chrome_options.add_argument("--disable-extensions")
    chrome_options.add_argument("--disable-popup-blocking")
    chrome_options.add_argument('--log-level=3')
    chrome_options.add_experimental_option('excludeSwitches', ['enable-logging'])


    logger.info("Inicializando WebDriver para Chrome en modo Headless...")
    driver = webdriver.Chrome(options=chrome_options)
    logger.info("WebDriver de Chrome inicializado correctamente.")
    wait = WebDriverWait(driver, 15)

except Exception as e:
    logger.exception(f"Error CRÍTICO al inicializar WebDriver: {e}")
    sys.exit(1)

# --- Flujo Principal ---
# --- CAMBIO: Bucle infinito externo ---
while True:
    stuck_counter = 0
    last_processed_location_str = ""
    sweep_start_time = time.time() # Registrar inicio del barrido

    try:
        logger.warning(f"\n{'='*10} INICIANDO NUEVO BARRIDO COMPLETO {'='*10}")
        # Cargar eventos al inicio de cada barrido para actualizar contadores
        load_events()

        logger.info("\n--- Ajuste de Vista Inicial para Nuevo Barrido ---")
        # Calcular centro del área para empezar desde allí
        initial_lat = (TARGET_AREA["lat_max"] + TARGET_AREA["lat_min"]) / 2
        initial_lon = (TARGET_AREA["lon_max"] + TARGET_AREA["lon_min"]) / 2
        logger.info(f"Moviendo al centro del área: Lat={initial_lat:.4f}, Lon={initial_lon:.4f}")

        current_lat, current_lon = move_to_coordinate(driver, wait, initial_lat, initial_lon, tolerance=0.005)

        if current_lat is None or current_lon is None:
            logger.critical("Advertencia CRÍTICA: No se pudo mover al centro del área. Usando coordenadas calculadas.")
            current_lat = initial_lat
            current_lon = initial_lon

        last_processed_location_str = f"{current_lat:.4f},{current_lon:.4f}" # Actualizar ubicación inicial

        # Ajustar pasos de movimiento (puedes ajustar esto según necesidad)
        STEPS_PER_MOVE_V = 10
        STEPS_PER_MOVE_H = 10
        logger.info(f"--- Usando {STEPS_PER_MOVE_V} V / {STEPS_PER_MOVE_H} H pasos por movimiento ---")

        logger.info("\n--- Iniciando Barrido Automático (Serpentina) ---")
        direction_v = Keys.ARROW_DOWN
        max_consecutive_failures = 5
        consecutive_failures = 0
        keep_running_this_sweep = True # Control para el barrido actual

        # --- Bucle Principal de Barrido (Serpentina) ---
        while keep_running_this_sweep and current_lon < TARGET_AREA["lon_max"]:
            while keep_running_this_sweep and ((direction_v == Keys.ARROW_DOWN and current_lat > TARGET_AREA["lat_min"]) or \
                  (direction_v == Keys.ARROW_UP and current_lat < TARGET_AREA["lat_max"])):

                current_location_str = f"{current_lat:.4f},{current_lon:.4f}"
                logger.info(f"\n--- [Guardados: {len(scraped_events)}] [Loc:{current_location_str}] Buscando ---")
                added_event_in_view = False
                processed_marker_this_pass = False

                try:
                    dismiss_recurring_overlays(driver, wait_time=0.5)
                    time.sleep(0.2)

                    markers = driver.find_elements(By.CSS_SELECTOR, MARKER_SELECTOR)
                    visible_markers = markers

                    if not visible_markers:
                        processed_marker_this_pass = True

                    marker_indices = list(range(len(visible_markers)))

                    for i in marker_indices:
                        # No necesitamos comprobar keep_running aquí, el bucle externo lo hará

                        try:
                            current_marker_element = driver.find_elements(By.CSS_SELECTOR, MARKER_SELECTOR)[i]
                            if not current_marker_element.is_displayed(): continue
                            marker_classes = current_marker_element.get_attribute("class")

                            if "wm-alert-cluster-icon" in marker_classes: pass
                            elif "wm-alert-icon" in marker_classes:
                                if scrape_event_details(driver, wait, current_marker_element):
                                    added_event_in_view = True
                                processed_marker_this_pass = True
                                time.sleep(0.2)

                        except (IndexError, StaleElementReferenceException): continue
                        except Exception as marker_proc_err:
                             logger.error(f"  -> Error procesando marcador específico ({i+1}): {marker_proc_err}")
                             continue

                    # Lógica Anti-Atasco
                    if processed_marker_this_pass:
                        if not added_event_in_view and visible_markers:
                            if current_location_str == last_processed_location_str:
                                stuck_counter += 1
                                logger.warning(f"  -> Vista sin eventos nuevos añadidos en la misma ubicación. Contador atasco: {stuck_counter}/{STUCK_THRESHOLD}")
                            else:
                                stuck_counter = 0
                                last_processed_location_str = current_location_str
                        elif added_event_in_view:
                            stuck_counter = 0
                            last_processed_location_str = current_location_str
                            # logger.info(f"  -> Vista procesada. Al menos 1 evento añadido.") # Log opcional
                        consecutive_failures = 0
                    else:
                         if visible_markers:
                             logger.warning("  -> ADVERTENCIA: No se procesó ningún marcador útil.")
                             consecutive_failures += 1


                    # Comprobar atasco
                    if stuck_counter >= STUCK_THRESHOLD:
                        logger.error(f"¡¡¡ATASCO DETECTADO!!! Intentando saltar...")
                        jump_lat = TARGET_AREA['lat_max'] - (current_lat - TARGET_AREA['lat_min'])
                        jump_lon = TARGET_AREA['lon_max'] - (current_lon - TARGET_AREA['lon_min'])
                        jump_lat = max(TARGET_AREA['lat_min'], min(TARGET_AREA['lat_max'], jump_lat))
                        jump_lon = max(TARGET_AREA['lon_min'], min(TARGET_AREA['lon_max'], jump_lon))
                        logger.warning(f"Intentando saltar a: Lat={jump_lat:.4f}, Lon={jump_lon:.4f}")
                        move_result = move_to_coordinate(driver, wait, jump_lat, jump_lon)
                        if move_result is not None:
                            current_lat, current_lon = move_result
                            logger.info("Salto completado.")
                            stuck_counter = 0
                            last_processed_location_str = f"{current_lat:.4f},{current_lon:.4f}"
                            consecutive_failures = 0
                        else:
                            logger.critical("Error crítico: Falló el salto anti-atasco. Abortando barrido actual.")
                            keep_running_this_sweep = False; break
                        # No necesitamos 'continue', el bucle while se re-evaluará

                    # Comprobar fallos generales
                    if consecutive_failures >= max_consecutive_failures:
                         logger.error(f"Error: {max_consecutive_failures} fallos generales consecutivos. Abortando barrido actual.")
                         keep_running_this_sweep = False; break

                    # Mover el mapa (si no estamos atascados y no se alcanzó el límite)
                    if keep_running_this_sweep:
                        direction_name = str(direction_v).replace('Keys.ARROW_', '')
                        # logger.info(f"  Moviendo mapa {direction_name} ({STEPS_PER_MOVE_V} pasos)...") # Menos verboso
                        move_result = pan_map(driver, wait, direction_v, STEPS_PER_MOVE_V)
                        if move_result is not None:
                            new_lat, new_lon = move_result
                            if f"{new_lat:.4f},{new_lon:.4f}" == current_location_str:
                                logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió después de mover {direction_name}.")
                                consecutive_failures += 1
                            else:
                                logger.debug(f"  -> Nueva ubicación: Lat={new_lat:.4f}, Lon={new_lon:.4f}")
                            current_lat, current_lon = new_lat, new_lon
                        else:
                            logger.critical("Error crítico al mover/obtener ubicación vertical. Abortando barrido actual.")
                            keep_running_this_sweep = False; break

                except TimeoutException:
                    logger.warning("  Timeout esperando marcadores. Moviendo...")
                    consecutive_failures += 1
                    if consecutive_failures >= max_consecutive_failures:
                         logger.error(f"Error: {max_consecutive_failures} timeouts consecutivos. Abortando barrido actual.")
                         keep_running_this_sweep = False; break

                    if keep_running_this_sweep:
                        direction_name = str(direction_v).replace('Keys.ARROW_', '')
                        # logger.info(f"  Moviendo mapa {direction_name} ({STEPS_PER_MOVE_V} pasos) (después de timeout)...")
                        move_result = pan_map(driver, wait, direction_v, STEPS_PER_MOVE_V)
                        if move_result is not None:
                            new_lat, new_lon = move_result
                            if f"{new_lat:.4f},{new_lon:.4f}" == current_location_str:
                                logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió después de mover {direction_name} (post-timeout).")
                                consecutive_failures += 1
                            else:
                                logger.debug(f"  -> Nueva ubicación: Lat={new_lat:.4f}, Lon={new_lon:.4f}")
                            current_lat, current_lon = new_lat, new_lon
                        else: keep_running_this_sweep = False; break

                except Exception as e:
                    logger.exception(f"Error inesperado en bucle de barrido vertical: {e}")
                    keep_running_this_sweep = False; break
            # Fin del bucle while vertical

            if not keep_running_this_sweep: break # Salir del bucle while horizontal

            # Mover a la derecha y cambiar dirección vertical
            logger.info(f"\n--- Fin de columna vertical. Moviendo a la derecha ({STEPS_PER_MOVE_H} pasos) ---")
            move_result_h = pan_map(driver, wait, Keys.ARROW_RIGHT, STEPS_PER_MOVE_H)
            if move_result_h is not None:
                new_lat, new_lon = move_result_h
                if f"{new_lat:.4f},{new_lon:.4f}" == current_location_str:
                     logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió después de mover DERECHA.")
                     consecutive_failures += 1 # Contar como fallo si no se mueve
                else:
                     logger.debug(f"  -> Nueva ubicación: Lat={new_lat:.4f}, Lon={new_lon:.4f}")
                current_lat, current_lon = new_lat, new_lon # Actualizar ambas por si acaso

                if current_lon >= TARGET_AREA["lon_max"]:
                    logger.warning("--- Límite Este del área alcanzado. Finalizando ESTE barrido. ---")
                    # No cambiar keep_running_this_sweep, el bucle while horizontal terminará
            else:
                 logger.critical("Error crítico al mover a la derecha. Abortando barrido actual.")
                 keep_running_this_sweep = False; break

            if keep_running_this_sweep:
                if direction_v == Keys.ARROW_DOWN: direction_v = Keys.ARROW_UP
                else: direction_v = Keys.ARROW_DOWN
                # logger.info(f"--- Invirtiendo dirección vertical a {str(direction_v).split('.')[-1]} ---") # Menos verboso

        # Fin del bucle while horizontal (fin de un barrido completo)
        sweep_duration = time.time() - sweep_start_time
        logger.warning(f"\n{'='*10} BARRIDO COMPLETO FINALIZADO EN {sweep_duration:.1f} SEGUNDOS {'='*10}")
        logger.warning(f"Total eventos guardados hasta ahora: {len(scraped_events)}")

        # Pausa antes del siguiente barrido
        logger.warning(f"Esperando {SLEEP_BETWEEN_SWEEPS} segundos antes del próximo barrido...")
        time.sleep(SLEEP_BETWEEN_SWEEPS)

except KeyboardInterrupt:
     logger.warning("\n--- Interrupción por teclado detectada ---")
except Exception as e:
    logger.exception(f"\n--- ERROR GENERAL DEL SCRIPT (FUERA DEL BUCLE PRINCIPAL): {e} ---")
    traceback.print_exc()

finally:
    logger.info("\n--- Finalizando Script ---")
    save_events() # Guardar estado final
    logger.warning(f"Total de eventos guardados en el archivo: {len(scraped_events)}")
    logger.warning(f"Total de IDs únicos encontrados: {len(event_id_counts)}")
    logger.warning(f"Resultados guardados en {output_filename}")
    logger.warning(f"Log completo guardado en {LOG_FILENAME}")

    if driver:
        logger.info("Cerrando navegador...")
        driver.quit()
    logging.shutdown() # Asegurar que los logs se escriban
    logger.info("Script terminado.")

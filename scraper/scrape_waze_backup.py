# -*- coding: utf-8 -*-

import time
import re
import json
import sys
import traceback
import random
import logging
from datetime import datetime
import os
from collections import defaultdict
# import tempfile # Eliminado: ya no gestionamos user-data-dir manualmente
# import shutil   # Eliminado: ya no gestionamos user-data-dir manualmente

# Intenta importar Selenium, sal si no está instalado
try:
    from selenium import webdriver
    from selenium.webdriver.common.by import By
    from selenium.webdriver.common.keys import Keys
    from selenium.webdriver.chrome.options import Options as ChromeOptions
    from selenium.webdriver.support.ui import WebDriverWait
    from selenium.webdriver.support import expected_conditions as EC
    from selenium.common.exceptions import (
        NoSuchElementException,
        TimeoutException,
        StaleElementReferenceException,
        ElementClickInterceptedException,
        ElementNotInteractableException,
        WebDriverException # Para errores de navegador/driver
    )
    from selenium.webdriver.common.action_chains import ActionChains
except ModuleNotFoundError:
    print("=" * 60)
    print("ERROR: El módulo 'selenium' no está instalado. Por favor, instálalo: pip install selenium")
    print("=" * 60)
    sys.exit(1)

# --- Configuración de Logging ---
OUTPUT_DATA_DIR = "/app/data"
LOG_FILENAME = os.path.join(OUTPUT_DATA_DIR, "log.txt")
SCREENSHOT_DIR = os.path.join(OUTPUT_DATA_DIR, "screenshots")

# Crear directorios si no existen
os.makedirs(OUTPUT_DATA_DIR, exist_ok=True)
os.makedirs(SCREENSHOT_DIR, exist_ok=True)

# Configuración básica del logging (se llama solo una vez)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILENAME, encoding="utf-8"),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)


# Función take_screenshot debe estar definida ANTES de ser llamada.
def take_screenshot(driver_instance, filename_prefix="debug_screenshot"):
    """Toma una captura de pantalla y la guarda en el directorio de screenshots."""
    if driver_instance:
        try:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filepath = os.path.join(SCREENSHOT_DIR, f"{filename_prefix}_{timestamp}.png")
            driver_instance.save_screenshot(filepath)
            logger.info(f"Captura de pantalla guardada en: {filepath}")
            return filepath
        except Exception as e:
            logger.error(f"Error al tomar captura de pantalla: {e}")
            return None
    else:
        logger.warning("No se puede tomar captura de pantalla: el driver no está inicializado.")
        return None

# --- Configuración del Script ---
WAZE_URL = "https://www.waze.com/es-419/live-map/"
TARGET_AREA = {
    "lat_max": -33.3503,  # Norte
    "lat_min": -33.6106,  # Sur
    "lon_min": -70.7778,  # Oeste
    "lon_max": -70.4990,  # Este
}
TARGET_ZOOM = 16
logger.info(f"Usando nivel de zoom objetivo: {TARGET_ZOOM}")
CLICKS_FOR_TARGET_ZOOM = 2

MAX_EVENTS = 10000
logger.info(f"Objetivo informativo de eventos a recolectar: {MAX_EVENTS}")

STUCK_THRESHOLD = 5
logger.info(f"Umbral anti-atasco (vistas sin eventos nuevos): {STUCK_THRESHOLD}")

SLEEP_BETWEEN_SWEEPS = 300
logger.info(f"Pausa entre barridos completos: {SLEEP_BETWEEN_SWEEPS} segundos")

MAP_SELECTOR = "#map"
MARKER_SELECTOR = "div.leaflet-marker-icon.wm-alert-icon.leaflet-interactive, div.leaflet-marker-icon.wm-alert-cluster-icon.leaflet-interactive"
LOCATION_SELECTOR = "div.wm-attribution-control__latlng > span"
POPUP_DETAILS_SELECTOR = "div.wm-alert-details"
TYPE_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > h4"
ADDRESS_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__address"
DATE_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__reporter > div > div.wm-alert-details__time"
REPORTER_SELECTOR = f"{POPUP_DETAILS_SELECTOR} > div.wm-alert-details__reporter > div > div.wm-alert-details__reporter-name > b"
CLOSE_POPUP_SELECTOR = "#map div.leaflet-popup-pane > div > a"
logger.info(f"Usando selector para cerrar popup de evento: {CLOSE_POPUP_SELECTOR}")
OVERLAY_SELECTOR = "div.waze-tour-step__overlay"
OVERLAY_CLOSE_BUTTON_SELECTOR = "button.waze-tour-step__close-button"
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

# Selectores para elementos que bloquean o que deben ser cerrados
WZ_SEARCH_FROM_TO_SELECTOR = "div.wz-search-from-to"
WZ_SEARCH_FROM_TO_CLOSE_BUTTON_SELECTOR = "div.wz-search-from-to button.close-button"
WZ_SEARCH_INPUT_SELECTOR = "input.wm-search__input[placeholder='Elige el destino']"
WM_ROUTING_TITLE_SELECTOR = "div.wm-routing__title"
WM_ROUTING_CLOSE_BUTTON_SELECTOR = f"{WM_ROUTING_TITLE_SELECTOR} > button"

scraped_events = []
event_id_counts = defaultdict(int)

output_filename = os.path.join(OUTPUT_DATA_DIR, "waze_events.json")
logger.info(f"Los eventos se guardarán en: {output_filename}")


def click_random_empty_space(driver_instance):
    """Intenta hacer clic en una coordenada aleatoria del viewport para cerrar popups."""
    try:
        window_width = driver_instance.execute_script("return window.innerWidth;")
        window_height = driver_instance.execute_script("return window.innerHeight;")
        
        x = random.randint(int(window_width * 0.1), int(window_width * 0.9))
        y = random.randint(int(window_height * 0.1), int(window_height * 0.9))
        
        logger.debug(f"  [ClickEmpty] Intentando clic en espacio vacío: ({x}, {y})")
        ActionChains(driver_instance).move_by_offset(x, y).click().perform()
        ActionChains(driver_instance).move_by_offset(-x, -y).perform() 
        time.sleep(0.3)
        return True
    except Exception as e:
        logger.debug(f"  [ClickEmpty] Fallo al hacer clic en espacio vacío: {e}")
        return False

def dismiss_recurring_overlays(driver_instance, wait_time=0.5):
    """
    Intenta cerrar cualquier overlay recurrente (tours, popups, barras de búsqueda) que intercepten clics.
    Devuelve True si se cerró algo significativo (clic en elemento explícito o ocultación con JS),
    False en caso contrario. Los ESC y clic en espacio vacío no garantizan True por sí mismos.
    """
    something_significant_was_closed = False

    close_button_selectors_and_actions = [
        (By.CSS_SELECTOR, OVERLAY_CLOSE_BUTTON_SELECTOR),
        (By.CSS_SELECTOR, WZ_SEARCH_FROM_TO_CLOSE_BUTTON_SELECTOR),
        (By.CSS_SELECTOR, WM_ROUTING_CLOSE_BUTTON_SELECTOR),
        (By.CSS_SELECTOR, COOKIE_ACCEPT_SELECTOR),
        (By.CSS_SELECTOR, AD_BANNER_CLOSE_SELECTOR),
        (By.CSS_SELECTOR, FILTERCLOSEPART1),
        (By.CSS_SELECTOR, FILTERCLOSEPART3),
        (By.XPATH, "//button[contains(text(),'Entendido') or contains(text(),'Got it') or contains(text(),'Ok') or contains(text(),'Aceptar')]"),
        (By.XPATH, "//button[contains(@aria-label,'Close') or contains(@aria-label,'Cerrar')]"),
        (By.CSS_SELECTOR, "a.close-button"),
    ]

    for by_method, selector in close_button_selectors_and_actions:
        try:
            close_element = WebDriverWait(driver_instance, 0.5).until(
                EC.element_to_be_clickable((by_method, selector))
            )
            if close_element.is_displayed():
                logger.info(f"  [Overlay] Encontrado y clickeable: '{selector}'. Intentando clic...")
                driver_instance.execute_script("arguments[0].click();", close_element)
                logger.info(f"  [Overlay] Clic en '{selector}' (o intento).")
                time.sleep(0.3)
                something_significant_was_closed = True
            else:
                logger.debug(f"  [Overlay] Elemento '{selector}' encontrado pero no visible.")
        except (NoSuchElementException, TimeoutException, ElementClickInterceptedException):
            logger.debug(f"  [Overlay] Elemento '{selector}' no encontrado o no clickeable (espera 0.5s).")
            pass
        except Exception as e:
            logger.error(f"  [Overlay] Error inesperado al intentar clic en '{selector}': {e}")
            take_screenshot(driver_instance, f"overlay_click_err_{selector.replace(' ','_').replace('>','').replace('#','')}")

    elements_to_hide_with_js = [
        WZ_SEARCH_FROM_TO_SELECTOR,
        WZ_SEARCH_INPUT_SELECTOR,
        WM_ROUTING_TITLE_SELECTOR
    ]
    for selector_to_hide in elements_to_hide_with_js:
        try:
            element_to_hide = WebDriverWait(driver_instance, 0.5).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, selector_to_hide))
            )
            if element_to_hide.is_displayed():
                logger.info(f"  [Overlay] Detectado elemento visible '{selector_to_hide}'. Intentando ocultar con JS.")
                driver_instance.execute_script("arguments[0].style.display = 'none';", element_to_hide)
                logger.info(f"  [Overlay] Elemento '{selector_to_hide}' oculto (o intento).")
                time.sleep(0.3)
                something_significant_was_closed = True
            else:
                logger.debug(f"  [Overlay] Elemento '{selector_to_hide}' encontrado pero no visible.")
        except (NoSuchElementException, TimeoutException):
            logger.debug(f"  [Overlay] Elemento '{selector_to_hide}' no presente o timeout (espera 0.5s).")
            pass
        except Exception as e:
            logger.error(f"  [Overlay] Error al ocultar '{selector_to_hide}' con JS: {e}")
            take_screenshot(driver_instance, f"hide_js_err_{selector_to_hide.replace(' ','_').replace('>','').replace('#','')}")

    try:
        ActionChains(driver_instance).send_keys(Keys.ESCAPE).perform()
        logger.debug("  [Overlay] Presionado ESC para intentar cerrar overlays (sin garantía de cierre real).")
        time.sleep(0.3)
    except Exception as esc_err:
        logger.debug(f"  [Overlay] Error al presionar ESC: {esc_err}")

    if click_random_empty_space(driver_instance):
        logger.debug("  [Overlay] Clic en espacio vacío realizado (sin garantía de cierre real).")

    return something_significant_was_closed
    
def dismiss_initial_elements(driver_instance):
    """
    Intenta cerrar varios elementos iniciales (publicidad, tooltips, cookies, filtros)
    que pueden aparecer al cargar Waze y bloquear la interacción.
    Este es un bucle que se repite hasta que no se cierra nada en una pasada.
    """
    logger.info("\n--- Intentando cerrar elementos iniciales (Ads, Tooltips, Cookies, Filtros)... ---")
    total_dismissed_elements = 0
    
    while True:
        current_iteration_closed_something = False
        iteration_start_count = total_dismissed_elements

        if dismiss_recurring_overlays(driver_instance, wait_time=1.0):
            current_iteration_closed_something = True
            total_dismissed_elements += 1
            time.sleep(0.8)

        # FILTRO 2 (Shadow DOM)
        try:
            filter2_host = WebDriverWait(driver_instance, 1).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, FILTER2_HOST_SELECTOR))
            )
            if filter2_host.is_displayed():
                logger.info(f"  [Initial] Detectado HOST de filtro 2 (Shadow DOM). Intentando clic...")
                click_script = f"""
                    const host = arguments[0];
                    const targetElement = host.shadowRoot.querySelector('{FILTER2_SHADOW_TARGET_SELECTOR}');
                    if (targetElement) {{ targetElement.click(); return true; }} else {{ return false; }}
                """
                clicked = driver_instance.execute_script(click_script, filter2_host)
                if clicked:
                    logger.info("  [Initial] Filtro 2 (Shadow DOM) clickeado (o intento).")
                    time.sleep(0.3)
                    current_iteration_closed_something = True
                    total_dismissed_elements += 1
                else:
                    logger.debug(f"  [Initial] No se encontró target para filtro 2 en Shadow DOM o no se pudo clickear.")
        except (NoSuchElementException, TimeoutException):
            pass
        except Exception as e:
            logger.error(f"  [Initial] Error al interactuar con filtro 2 (Shadow DOM): {e}")
            take_screenshot(driver_instance, "filter2_shadow_dom_error")

        if not current_iteration_closed_something and total_dismissed_elements > iteration_start_count: 
            logger.info("  [Initial Dismiss] No se cerraron más elementos en esta iteración. Fin de intentos.")
            break
        elif not current_iteration_closed_something and total_dismissed_elements == iteration_start_count:
            logger.info("  [Initial Dismiss] No se cerró nada y no se había cerrado nada antes en esta pasada. Terminando.")
            break
        
        time.sleep(0.5)

    logger.info(f"--- Cierre de elementos iniciales finalizado ({total_dismissed_elements} elementos gestionados) ---")


def set_initial_zoom(driver_instance, clicks_needed):
    """
    Realiza clics en el botón de zoom in para alcanzar el nivel de zoom objetivo.
    Maneja overlays interceptores de clics.
    """
    logger.info(f"\n--- Intentando ajustar zoom inicial con {clicks_needed} clics ---")
    zoom_success = True
    zoom_in_button_locator = (By.CSS_SELECTOR, ZOOM_IN_BUTTON_SELECTOR)

    for i in range(clicks_needed):
        logger.info(f"  [Zoom] Intentando clic de zoom #{i+1}/{clicks_needed}...")
        attempt = 0
        max_attempts_per_zoom_click = 3
        clicked_this_zoom_step = False

        while attempt < max_attempts_per_zoom_click and not clicked_this_zoom_step:
            attempt += 1
            logger.debug(f"    [Zoom Clic #{i+1}] Intento #{attempt}...")
            try:
                if dismiss_recurring_overlays(driver_instance, wait_time=0.5):
                    time.sleep(0.5)

                zoom_in_button = WebDriverWait(driver_instance, 3).until(
                    EC.element_to_be_clickable(zoom_in_button_locator)
                )
                zoom_in_button.click()
                logger.info(f"    [Zoom Clic #{i+1}] Clic normal exitoso.")
                clicked_this_zoom_step = True
                time.sleep(1.2)

            except ElementClickInterceptedException as eci:
                error_msg_short = str(eci).splitlines()[0]
                logger.warning(f"    [Zoom Clic #{i+1}] Clic interceptado (Intento {attempt}): {error_msg_short}")
                take_screenshot(driver_instance, f"zoom_click_intercepted_{i+1}_attempt{attempt}")
                logger.info("      Intentando cerrar overlay y reintentar con JS...")
                if dismiss_recurring_overlays(driver_instance, wait_time=1):
                    time.sleep(0.8)
                try:
                    zoom_in_button_for_js = WebDriverWait(driver_instance, 2).until(
                        EC.presence_of_element_located(zoom_in_button_locator)
                    )
                    driver_instance.execute_script("arguments[0].click();", zoom_in_button_for_js)
                    logger.info(f"    [Zoom Clic #{i+1}] Clic con JS exitoso después de intercepción.")
                    clicked_this_zoom_step = True
                    time.sleep(1.2)
                except Exception as js_err:
                    logger.error(f"    [Zoom Clic #{i+1}] Clic con JS también falló: {js_err}")
                    take_screenshot(driver_instance, f"zoom_click_js_fail_{i+1}_attempt{attempt}")
            
            except (NoSuchElementException, TimeoutException) as e_find:
                logger.error(f"    [Zoom Clic #{i+1}] No se encontró o no fue clickeable el botón de zoom (Intento {attempt+1}): {e_find}")
                take_screenshot(driver_instance, f"zoom_button_not_found_{i+1}_attempt{attempt}")
            
            except Exception as e_other:
                logger.error(f"    [Zoom Clic #{i+1}] Error inesperado durante clic de zoom (Intento {attempt+1}): {e_other}")
                take_screenshot(driver_instance, f"zoom_unexpected_error_{i+1}_attempt{attempt}")
            
            if not clicked_this_zoom_step and attempt < max_attempts_per_zoom_click:
                logger.debug(f"    [Zoom Clic #{i+1}] Reintentando en 0.5s...")
                time.sleep(0.5)

        if not clicked_this_zoom_step:
            logger.error(f"  [Zoom] Falló el clic de zoom #{i+1} después de {max_attempts_per_zoom_click} intentos.")
            zoom_success = False
            break

    if zoom_success:
        logger.info(f"--- Ajuste de zoom inicial completado ---")
    else:
        logger.warning(f"--- Ajuste de zoom inicial PARCIALMENTE o COMPLETAMENTE fallido ---")
    return zoom_success


def get_current_location(driver_instance, retries=2, delay=0.4):
    """
    Obtiene la latitud y longitud actuales del mapa.
    Reintenta si el texto no cambia o si hay errores.
    """
    last_location_text = None
    for attempt in range(retries + 1):
        try:
            location_wait = WebDriverWait(driver_instance, 3)
            location_element = location_wait.until(
                EC.visibility_of_element_located((By.CSS_SELECTOR, LOCATION_SELECTOR))
            )
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
            if attempt < retries:
                time.sleep(delay)
            else:
                return None, None
        except Exception as e:
            logger.exception(f"Error inesperado al obtener ubicación: {e}")
            return None, None
    return None, None

def pan_map(driver_instance, direction_key, steps=1):
    """
    Mueve el mapa usando las teclas de flecha.
    Maneja ElementNotInteractableException intentando cerrar overlays.
    """
    map_element = None
    try:
        dismiss_recurring_overlays(driver_instance, wait_time=0.5)
        map_element = driver_instance.find_element(By.CSS_SELECTOR, MAP_SELECTOR)

        actions = ActionChains(driver_instance)
        actions.move_to_element(map_element).click().perform()

        for _ in range(steps):
            actions = ActionChains(driver_instance)
            actions.send_keys(direction_key).perform()
            time.sleep(0.2)

        logger.debug(f"  [Pan] Ejecutando movimiento: {str(direction_key).split('.')[-1]} x {steps}")
        time.sleep(2.0)

        lat, lon = get_current_location(driver_instance, retries=2, delay=0.6)
        if lat is not None:
            logger.debug(f"  [Pan] Nueva ubicación post-movimiento: Lat={lat:.4f}, Lon={lon:.4f}")
        else:
            logger.warning("  [Pan] No se pudo obtener ubicación después del movimiento.")
        return lat, lon

    except ElementNotInteractableException:
        logger.warning("  [Pan] Error: Mapa no interactuable. Intentando cerrar overlays y reintentar UNA VEZ...")
        take_screenshot(driver_instance, "pan_not_interactable")
        if dismiss_recurring_overlays(driver_instance, wait_time=0.5):
            time.sleep(0.3)
            try:
                map_element = driver_instance.find_element(By.CSS_SELECTOR, MAP_SELECTOR)
                actions = ActionChains(driver_instance)
                actions.move_to_element(map_element).click().perform()
                for _ in range(steps):
                    actions = ActionChains(driver_instance)
                    actions.send_keys(direction_key).perform()
                    time.sleep(0.2)
                logger.debug(f"  [Pan] Ejecutando REINTENTO de movimiento: {str(direction_key).split('.')[-1]} x {steps}")
                time.sleep(2.0)
                lat, lon = get_current_location(driver_instance, retries=2, delay=0.6)
                if lat is not None:
                    logger.debug(f"  [Pan] Nueva ubicación post-reintento: Lat={lat:.4f}, Lon={lon:.4f}")
                else:
                    logger.warning("  [Pan] No se pudo obtener ubicación después del reintento.")
                return lat, lon
            except Exception as retry_err:
                logger.error(f"  [Pan] Reintento de movimiento falló: {retry_err}.")
                take_screenshot(driver_instance, "pan_retry_fail")
                return None, None
        else:
            logger.warning("  [Pan] No se pudieron cerrar overlays durante el reintento.")
            return None, None

    except (NoSuchElementException, StaleElementReferenceException) as e:
        logger.error(f"  [Pan] Error localizando el mapa: {e}")
        take_screenshot(driver_instance, "pan_map_not_found")
    except Exception as e:
        logger.exception(f"  [Pan] Error inesperado al mover mapa: {e}")
        take_screenshot(driver_instance, "pan_unexpected_error")
    return None, None

def close_popup(driver_instance):
    """
    Intenta cerrar el popup de detalles de un evento.
    """
    popup_closed = False
    time.sleep(0.5)

    if CLOSE_POPUP_SELECTOR:
        try:
            close_button = WebDriverWait(driver_instance, 3).until(
                EC.element_to_be_clickable((By.CSS_SELECTOR, CLOSE_POPUP_SELECTOR))
            )
            try:
                close_button.click()
            except ElementClickInterceptedException:
                logger.warning(f"  -> Clic en botón de cerrar popup interceptado. Intentando JS.")
                driver_instance.execute_script("arguments[0].click();", close_button)
            except Exception as e_click:
                logger.warning(f"  -> Error al clickear botón de cerrar popup: {e_click}. Intentando JS.")
                driver_instance.execute_script("arguments[0].click();", close_button)
            
            popup_closed = True
            time.sleep(0.3)
        except TimeoutException:
            logger.debug("  -> Botón de cerrar popup no encontrado, puede que ya se haya cerrado.")
            pass
        except StaleElementReferenceException:
            logger.warning("  -> Botón de cerrar popup se volvió obsoleto. Asumiendo cerrado.")
            popup_closed = True
        except Exception as e:
            logger.error(f"  -> Error inesperado al intentar cerrar popup: {e}")
            take_screenshot(driver_instance, "close_popup_fail")

    if not popup_closed:
        if dismiss_recurring_overlays(driver_instance, wait_time=0.5):
            logger.info("  -> Popup cerrado por método genérico (ESC/clic en espacio vacío).")
            popup_closed = True

    return popup_closed


def try_click(driver_instance, element_or_locator):
    """
    Intenta hacer clic en un elemento, con reintentos y manejo de overlays.
    """
    logger.debug(f"Intentando clic en: {element_or_locator}")
    max_click_attempts = 3
    for attempt in range(max_click_attempts):
        try:
            if isinstance(element_or_locator, tuple):
                target_element = WebDriverWait(driver_instance, 5).until(
                    EC.element_to_be_clickable(element_or_locator)
                )
            else:
                target_element = WebDriverWait(driver_instance, 5).until(
                    EC.element_to_be_clickable(element_or_locator)
                )
            
            target_element.click()
            logger.debug(f"  -> Clic normal exitoso (intento {attempt+1}).")
            return True
        except ElementClickInterceptedException as eci:
            error_msg = str(eci).splitlines()[0]
            logger.warning(f"  -> Clic interceptado (intento {attempt+1}): {error_msg}")
            take_screenshot(driver_instance, "click_intercepted_marker")
            
            if dismiss_recurring_overlays(driver_instance, wait_time=1.0):
                time.sleep(0.8)
            
            if attempt < max_click_attempts - 1:
                logger.info("     Reintentando clic (próximo intento será con JS o re-localización).")
                continue
            else:
                logger.info("  -> Clic interceptado en el último intento normal, usando JS...")
                try:
                    if isinstance(element_or_locator, tuple):
                        element_for_js = WebDriverWait(driver_instance,3).until(EC.presence_of_element_located(element_or_locator))
                    else:
                        element_for_js = element_or_locator
                    driver_instance.execute_script("arguments[0].click();", element_for_js)
                    logger.debug("  -> Clic con JS exitoso.")
                    return True
                except Exception as js_err:
                    logger.error(f"  -> Clic con JS también falló: {js_err}")
                    take_screenshot(driver_instance, "click_intercepted_js_fail")
                    return False
        except (TimeoutException, StaleElementReferenceException, ElementNotInteractableException) as e_click:
            logger.warning(f"  -> Error durante el clic (intento {attempt+1}): {type(e_click).__name__} - {str(e_click).splitlines()[0]}")
            take_screenshot(driver_instance, "click_fail_element_state")
            if attempt >= max_click_attempts -1:
                return False
            time.sleep(0.5)
        except Exception as e_general:
            logger.exception(f"  -> Error inesperado durante el clic (intento {attempt+1}): {e_general}")
            take_screenshot(driver_instance, "click_unexpected_fail")
            return False
    return False

def scrape_event_details(driver_instance, event_element):
    """
    Hace clic en un elemento de evento, extrae sus detalles del popup y lo guarda.
    """
    global scraped_events, event_id_counts
    event_was_added = False

    dismiss_recurring_overlays(driver_instance, wait_time=0.5)
    time.sleep(0.3)

    if not try_click(driver_instance, event_element):
        logger.warning("  -> Falló el clic en el evento. Saltando.")
        return False

    try:
        details_popup_wait = WebDriverWait(driver_instance, 8)
        details_popup = details_popup_wait.until(
            EC.visibility_of_element_located((By.CSS_SELECTOR, POPUP_DETAILS_SELECTOR))
        )

        details_text = []
        event_type = "Desconocido"
        event_address = "No disponible"
        event_date_str = "No disponible"
        reporter_name = "Desconocido"

        try:
            event_type = details_popup.find_element(By.CSS_SELECTOR, TYPE_SELECTOR).text
            details_text.append(f"T:{event_type}")
        except NoSuchElementException:
            logger.debug("  -> Tipo de evento no encontrado en popup.")
        try:
            event_address = details_popup.find_element(By.CSS_SELECTOR, ADDRESS_SELECTOR).text
            details_text.append(f"D:{event_address}")
        except NoSuchElementException:
            logger.debug("  -> Dirección de evento no encontrada en popup.")
        try:
            event_date_str = details_popup.find_element(By.CSS_SELECTOR, DATE_SELECTOR).text
            details_text.append(f"H:{event_date_str}")
        except NoSuchElementException:
            logger.debug("  -> Fecha de evento no encontrada en popup.")
        try:
            reporter_name = details_popup.find_element(By.CSS_SELECTOR, REPORTER_SELECTOR).text
            details_text.append(f"R:{reporter_name}")
        except NoSuchElementException:
            logger.debug("  -> Reportero no encontrado en popup.")

        event_data = {
            "type": event_type,
            "address": event_address,
            "timestamp_original_relative": event_date_str,
            "reporter": reporter_name,
            "scrape_timestamp": datetime.now().isoformat()
        }

        event_id_parts = [
            event_data["reporter"] if event_data["reporter"] != "Desconocido" else "Anon",
            event_data["address"] if event_data["address"] != "No disponible" else "UnknownLoc",
            event_data["type"] if event_data["type"] != "Desconocido" else "UnknownType"
        ]
        event_id = "-".join(part.replace(" ", "_").replace("/", "_") for part in event_id_parts)
        event_data["event_id"] = event_id

        log_summary = " | ".join(filter(None, details_text))
        if logger.level <= logging.INFO:
            logger.info("     " + log_summary + f" | ID: {event_id}")

        current_count = event_id_counts.get(event_id, 0)
        if current_count < 5:
            scraped_events.append(event_data)
            event_id_counts[event_id] = current_count + 1
            event_was_added = True
            current_saved_count = len(scraped_events)
            logger.warning(f"--- Evento Guardado #{current_saved_count} ---")
            event_json_string = json.dumps(event_data, ensure_ascii=False)
            logger.warning(f"Datos: {event_json_string}")
            logger.warning("-" * 30)
            save_events()

            if current_saved_count % 100 == 0:
                logger.warning(f"--- Progreso: {current_saved_count} eventos guardados. ({len(event_id_counts)} IDs únicos encontrados) ---")
        else:
            if logger.level <= logging.INFO:
                logger.info(f"  -> Evento con ID '{event_id}' omitido (límite de {current_count}/5 repeticiones alcanzado).")
            event_was_added = False

        close_popup(driver_instance)

    except TimeoutException:
        logger.warning("  -> Error: Timeout esperando el popup de detalles después del clic.")
        take_screenshot(driver_instance, "details_popup_timeout")
    except StaleElementReferenceException:
        logger.warning("  -> Error: El popup o sus elementos se volvieron obsoletos.")
        take_screenshot(driver_instance, "details_popup_stale")
    except Exception as e:
        logger.exception(f"  -> Error inesperado extrayendo detalles o cerrando popup: {e}")
        take_screenshot(driver_instance, "details_popup_unexpected_error")

    return event_was_added


def click_cluster(driver_instance, cluster_element):
    """
    Hace clic en un elemento de cluster para expandirlo.
    """
    dismiss_recurring_overlays(driver_instance, wait_time=0.5)
    time.sleep(0.3)

    if not try_click(driver_instance, cluster_element):
        logger.warning("  -> Falló el clic en el cluster. Saltando.")
        return False

    logger.info("  -> Clic en cluster realizado.")
    time.sleep(1.0)
    return True


def save_events():
    """
    Guarda la lista de eventos raspados en el archivo JSON.
    Este archivo será consumido por el importador de MongoDB.
    """
    global scraped_events, output_filename
    try:
        output_dir_for_save = os.path.dirname(output_filename)
        if not os.path.exists(output_dir_for_save):
            os.makedirs(output_dir_for_save, exist_ok=True)
            logger.info(f"Directorio de salida '{output_dir_for_save}' creado.")

        with open(output_filename, "w", encoding="utf-8") as f:
            json.dump(scraped_events, f, ensure_ascii=False, indent=4)
    except Exception as e:
        logger.exception(f"  -> Error al guardar eventos en JSON ({output_filename}): {e}")

def load_events():
    """
    Carga eventos previamente raspados desde el archivo JSON si existe.
    Reconstruye las listas y contadores, aplicando límites de duplicados si aplica.
    """
    global scraped_events, event_id_counts
    # Resetear siempre para un nuevo barrido, ya que el importer moverá el archivo procesado.
    scraped_events = []
    event_id_counts.clear()
    logger.info(f"Listas de eventos y contadores reseteados para un nuevo barrido.")


def move_to_coordinate(driver_instance, target_lat, target_lon, max_steps=150, tolerance=0.008):
    """
    Mueve el mapa gradualmente hacia una coordenada objetivo.
    """
    logger.info(f"\n--- Intentando mover mapa a Lat={target_lat:.4f}, Lon={target_lon:.4f} (Tol: {tolerance}) ---")
    current_lat, current_lon = get_current_location(driver_instance, retries=3)
    if current_lat is None or current_lon is None:
        logger.error("Error: No se pudo obtener la ubicación actual para iniciar el movimiento.")
        return None, None

    steps_taken = 0
    while steps_taken < max_steps:
        time.sleep(0.1)
        dismiss_recurring_overlays(driver_instance, wait_time=0.5)
        lat_diff = target_lat - current_lat
        lon_diff = target_lon - current_lon

        if abs(lat_diff) < tolerance and abs(lon_diff) < tolerance:
            logger.info(f"--- Ubicación objetivo alcanzada (aprox.) después de {steps_taken} pasos ---")
            return current_lat, current_lon

        moved_in_this_step = False

        if abs(lat_diff) >= tolerance:
            direction_v_key = Keys.ARROW_UP if lat_diff > 0 else Keys.ARROW_DOWN
            move_result_v = pan_map(driver_instance, direction_v_key, 1)
            if move_result_v is not None:
                current_lat, current_lon = move_result_v
                moved_in_this_step = True
            else:
                logger.warning("  [MoveDebug] Fallo en movimiento vertical, deteniendo move_to_coordinate.")
                break

        if abs(lon_diff) >= tolerance:
            direction_h_key = Keys.ARROW_LEFT if lon_diff < 0 else Keys.ARROW_RIGHT
            move_result_h = pan_map(driver_instance, direction_h_key, 1)
            if move_result_h is not None:
                new_h_lat, new_h_lon = move_result_h
                current_lon = new_h_lon
                if not moved_in_this_step:
                    current_lat = new_h_lat
                moved_in_this_step = True
            else:
                logger.warning("  [MoveDebug] Fallo en movimiento horizontal, deteniendo move_to_coordinate.")
                break

        steps_taken += 1
        if not moved_in_this_step and steps_taken > 0:
            logger.error("Error: No se pudo realizar movimiento en pan_map (ambas direcciones fallaron o no eran necesarias).")
            break

    logger.info(f"--- Movimiento a coordenada detenido (max pasos o error). Posición final aprox: Lat={current_lat:.4f}, Lon={current_lon:.4f} ---")
    return current_lat, current_lon


# --- Flujo Principal de Ejecución ---
def main():
    global driver
    driver = None
    # user_data_dir = None # Eliminado: ya no gestionamos user-data-dir manualmente
    main_loop_running = True

    try:
        # user_data_dir = tempfile.mkdtemp() # Eliminado
        # logger.info(f"Usando directorio temporal para datos de usuario: {user_data_dir}") # Eliminado

        chrome_options = ChromeOptions()
        # === MODIFICACIÓN CLAVE: Habilitar modo headless moderno ===
        chrome_options.add_argument("--headless=new")
        # Las siguientes opciones se mantienen aunque headless las pueda ignorar, por compatibilidad o seguridad.
        chrome_options.add_argument("--no-first-run")
        chrome_options.add_argument("--disable-setuid-sandbox")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-gpu")
        chrome_options.add_argument("--window-size=1920,1080")
        chrome_options.add_argument("--lang=es-419")
        chrome_options.add_argument("user-agent=Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/108.0.0.0 Safari/537.36")
        chrome_options.add_argument("--disable-extensions")
        chrome_options.add_argument("--disable-popup-blocking")
        chrome_options.add_argument("--log-level=3")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
        
        # === Eliminamos la especificación explícita del user-data-dir temporal ===
        # chrome_options.add_argument(f"--user-data-dir={user_data_dir}") # Eliminado

        logger.info("Inicializando WebDriver para Chrome en modo Headless...")
        driver = webdriver.Chrome(options=chrome_options)
        logger.info("WebDriver de Chrome inicializado correctamente.")

        driver.get(WAZE_URL)
        
        time.sleep(random.uniform(5, 10))
        
        dismiss_initial_elements(driver)
        set_initial_zoom(driver, CLICKS_FOR_TARGET_ZOOM)

        while main_loop_running:
            stuck_counter = 0
            last_processed_location_str = ""
            sweep_start_time = time.time()

            try:
                logger.warning(f"\n{'='*10} INICIANDO NUEVO BARRIDO COMPLETO {'='*10}")
                load_events()

                logger.info("\n--- Ajuste de Vista Inicial para Nuevo Barrido ---")
                initial_lat = (TARGET_AREA["lat_max"] + TARGET_AREA["lat_min"]) / 2
                initial_lon = (TARGET_AREA["lon_max"] + TARGET_AREA["lon_min"]) / 2
                logger.info(f"Moviendo al centro del Área: Lat={initial_lat:.4f}, Lon={initial_lon:.4f}")

                current_lat, current_lon = move_to_coordinate(driver, initial_lat, initial_lon, tolerance=0.01)

                if current_lat is None or current_lon is None:
                    logger.critical("Advertencia CRÍTICA: No se pudo mover al centro del Área. Usando coordenadas calculadas.")
                    take_screenshot(driver, "move_to_center_failed")
                    current_lat, current_lon = initial_lat, initial_lon # Fallback

                last_processed_location_str = f"{current_lat:.4f},{current_lon:.4f}"

                STEPS_PER_MOVE_V = 8
                STEPS_PER_MOVE_H = 8
                logger.info(f"--- Usando {STEPS_PER_MOVE_V} V / {STEPS_PER_MOVE_H} H pasos por movimiento ---")

                logger.info("\n--- Iniciando Barrido Automático (Serpentina) ---")
                direction_v = Keys.ARROW_DOWN
                max_consecutive_failures = 5
                consecutive_failures = 0
                keep_running_this_sweep = True

                while keep_running_this_sweep and current_lon < TARGET_AREA["lon_max"]:
                    while keep_running_this_sweep and (
                        (direction_v == Keys.ARROW_DOWN and current_lat > TARGET_AREA["lat_min"])
                        or (direction_v == Keys.ARROW_UP and current_lat < TARGET_AREA["lat_max"])
                    ):

                        current_location_str = f"{current_lat:.4f},{current_lon:.4f}"
                        logger.info(f"\n--- [Guardados: {len(scraped_events)}] [Loc:{current_location_str}] Buscando ---")
                        added_event_in_view = False
                        processed_marker_this_pass = False

                        try:
                            dismiss_recurring_overlays(driver, wait_time=0.5)
                            time.sleep(0.5)

                            markers = WebDriverWait(driver, 8).until(
                                lambda d: d.find_elements(By.CSS_SELECTOR, MARKER_SELECTOR)
                            )

                            if not markers:
                                logger.info("  -> No se encontraron marcadores en esta vista.")
                                processed_marker_this_pass = True
                                if current_location_str == last_processed_location_str:
                                    stuck_counter += 1
                                else:
                                    stuck_counter = 0
                                    last_processed_location_str = current_location_str
                            else:
                                try:
                                    visible_markers = [m for m in driver.find_elements(By.CSS_SELECTOR, MARKER_SELECTOR) if m.is_displayed()]
                                    logger.debug(f"  -> {len(visible_markers)} marcadores visibles para procesar.")
                                except StaleElementReferenceException:
                                    logger.warning("  -> StaleElementReferenceException al re-localizar marcadores. Reintentando la vista.")
                                    time.sleep(1)
                                    continue

                                for i, current_marker_element in enumerate(visible_markers):
                                    if not keep_running_this_sweep:
                                        break
                                    try:
                                        marker_classes = current_marker_element.get_attribute("class")

                                        if "wm-alert-cluster-icon" in marker_classes:
                                            logger.info(f"  -> Marcador {i+1}/{len(visible_markers)} es un CLUSTER. Intentando clic...")
                                            if click_cluster(driver, current_marker_element):
                                                added_event_in_view = True
                                                processed_marker_this_pass = True
                                                stuck_counter = 0
                                                last_processed_location_str = f"{current_lat:.4f},{current_lon:.4f}"
                                                logger.info("  -> Re-escaneando marcadores después de clic en cluster...")
                                                time.sleep(0.5)
                                                break
                                            else:
                                                logger.warning(f"  -> Falló el clic en el cluster {i+1}.")
                                                take_screenshot(driver, f"cluster_click_fail_{i+1}")

                                        elif "wm-alert-icon" in marker_classes:
                                            logger.info(f"  -> Marcador {i+1}/{len(visible_markers)} es un EVENTO. Intentando extraer...")
                                            if scrape_event_details(driver, current_marker_element):
                                                added_event_in_view = True
                                            processed_marker_this_pass = True
                                            time.sleep(0.1)

                                    except StaleElementReferenceException:
                                        logger.debug(f"  -> Marcador {i+1} se volvió obsoleto. Saltando y re-escaneando.")
                                        break
                                    except Exception as marker_proc_err:
                                        logger.error(f"  -> Error procesando marcador específico ({i+1}): {marker_proc_err}")
                                        take_screenshot(driver, f"marker_process_error_{i+1}")
                                        continue

                            if processed_marker_this_pass:
                                if not added_event_in_view and markers:
                                    if current_location_str == last_processed_location_str:
                                        stuck_counter += 1
                                        logger.warning(f"  -> Vista con marcadores pero sin eventos NUEVOS. Contador atasco: {stuck_counter}/{STUCK_THRESHOLD}")
                                    else:
                                        stuck_counter = 0
                                        last_processed_location_str = current_location_str
                                elif added_event_in_view:
                                    stuck_counter = 0
                                    last_processed_location_str = current_location_str
                                consecutive_failures = 0
                            else:
                                if markers:
                                    logger.warning("  -> ADVERTENCIA: Hubo marcadores pero no se procesó ninguno útil.")
                                consecutive_failures += 1
                                take_screenshot(driver, "view_not_processed_useful")

                            if stuck_counter >= STUCK_THRESHOLD:
                                logger.error(f"¡¡¡ATASCO DETECTADO!!! ({stuck_counter}/{STUCK_THRESHOLD} vistas en misma loc sin progreso)")
                                take_screenshot(driver, "stuck_detected")
                                random_direction_key = random.choice([Keys.ARROW_UP, Keys.ARROW_DOWN, Keys.ARROW_LEFT, Keys.ARROW_RIGHT])
                                random_steps = random.randint(5, 15)
                                logger.warning(f"Intentando movimiento aleatorio para desatascar: {str(random_direction_key).split('.')[-1]} x {random_steps} pasos.")
                                move_result = pan_map(driver, random_direction_key, random_steps)
                                if move_result is not None:
                                    current_lat, current_lon = move_result
                                    logger.info("Movimiento aleatorio completado.")
                                    stuck_counter = 0
                                    last_processed_location_str = f"{current_lat:.4f},{current_lon:.4f}"
                                    consecutive_failures = 0
                                else:
                                    logger.critical("Error crítico: Falló el movimiento aleatorio anti-atasco. Abortando barrido actual.")
                                    take_screenshot(driver, "stuck_recovery_fail")
                                    keep_running_this_sweep = False
                                    break

                            if consecutive_failures >= max_consecutive_failures:
                                logger.error(f"Error: {max_consecutive_failures} fallos generales consecutivos procesando vistas. Abortando barrido actual.")
                                take_screenshot(driver, "consecutive_failures_limit")
                                keep_running_this_sweep = False
                                break

                            if keep_running_this_sweep:
                                direction_name = str(direction_v).replace("Keys.ARROW_", "")
                                move_result = pan_map(driver, direction_v, STEPS_PER_MOVE_V)
                                if move_result is not None:
                                    new_lat, new_lon = move_result
                                    if f"{new_lat:.4f},{new_lon:.4f}" == current_location_str and markers:
                                        logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió Y había marcadores después de mover {direction_name}.")
                                        consecutive_failures += 1
                                    else:
                                        logger.debug(f"  -> Nueva ubicación: Lat={new_lat:.4f}, Lon={new_lon:.4f}")
                                    current_lat, current_lon = new_lat, new_lon
                                else:
                                    keep_running_this_sweep = False
                                    break

                        except TimeoutException:
                            logger.warning("  Timeout esperando marcadores en la vista. Moviendo...")
                            take_screenshot(driver, "markers_timeout")
                            consecutive_failures += 1
                            if consecutive_failures >= max_consecutive_failures:
                                logger.error(f"Error: {max_consecutive_failures} timeouts consecutivos buscando marcadores. Abortando barrido actual.")
                                take_screenshot(driver, "markers_timeout_limit")
                                keep_running_this_sweep = False
                                break

                            if keep_running_this_sweep:
                                direction_name = str(direction_v).replace("Keys.ARROW_", "")
                                move_result = pan_map(driver, direction_v, STEPS_PER_MOVE_V)
                                if move_result is not None:
                                    new_lat, new_lon = move_result
                                    if f"{new_lat:.4f},{new_lon:.4f}" == current_location_str:
                                        logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió después de mover {direction_name} (post-timeout).")
                                        consecutive_failures += 1
                                    else:
                                        logger.debug(f"  -> Nueva ubicación: Lat={new_lat:.4f}, Lon={new_lon:.4f}")
                                    current_lat, current_lon = new_lat, new_lon
                                else:
                                    keep_running_this_sweep = False
                                    break

                        except Exception as e_vista:
                            logger.exception(f"Error inesperado en bucle de procesamiento de vista: {e_vista}")
                            take_screenshot(driver, "view_processing_unexpected_error")
                            consecutive_failures += 1
                            if consecutive_failures >= max_consecutive_failures:
                                logger.error(f"Error: {max_consecutive_failures} errores inesperados. Abortando barrido actual.")
                                keep_running_this_sweep = False
                    # Fin del bucle while vertical

                    if not keep_running_this_sweep:
                        break

                    # Mover a la derecha y cambiar dirección vertical
                    logger.info(f"\n--- Fin de columna vertical. Moviendo a la derecha ({STEPS_PER_MOVE_H} pasos) ---")
                    move_result_h = pan_map(driver, Keys.ARROW_RIGHT, STEPS_PER_MOVE_H)
                    if move_result_h is not None:
                        new_lat_h, new_lon_h = move_result_h
                        if f"{new_lat_h:.4f},{new_lon_h:.4f}" == current_location_str:
                            logger.warning(f"  -> ADVERTENCIA: La ubicación no cambió después de mover DERECHA.")
                            consecutive_failures += 1
                        else:
                            logger.debug(f"  -> Nueva ubicación: Lat={new_lat_h:.4f}, Lon={new_lon_h:.4f}")
                        current_lat, current_lon = new_lat_h, new_lon_h

                        if current_lon >= TARGET_AREA["lon_max"]:
                            logger.warning("--- Límite Este del Área alcanzado. Finalizando ESTE barrido. ---")
                    else:
                        logger.critical("Error crítico al mover a la derecha. Abortando barrido actual.")
                        take_screenshot(driver, "move_horizontal_fail")
                        keep_running_this_sweep = False
                        break

                    if keep_running_this_sweep:
                        if direction_v == Keys.ARROW_DOWN:
                            direction_v = Keys.ARROW_UP
                        else:
                            direction_v = Keys.ARROW_DOWN
            # Fin del bucle while horizontal (fin de un barrido completo)

            sweep_duration = time.time() - sweep_start_time
            logger.warning(f"\n{'='*10} BARRIDO COMPLETO FINALIZADO EN {sweep_duration:.1f} SEGUNDOS {'='*10}")
            logger.warning(f"Total eventos guardados en el archivo en esta corrida: {len(scraped_events)}")

            logger.warning(f"Esperando {SLEEP_BETWEEN_SWEEPS} segundos antes del próximo barrido...")
            time.sleep(SLEEP_BETWEEN_SWEEPS)

    except KeyboardInterrupt:
        logger.warning("\n--- Interrupción por teclado durante un barrido ---")
        main_loop_running = False
    except WebDriverException as e_driver:
        logger.critical(f"\n--- ERROR CRÍTICO DE WEBDRIVER: El navegador falló o no se pudo inicializar: {e_driver} ---")
        take_screenshot(driver, "webdriver_critical_error")
    except Exception as e_global:
        logger.exception(f"\n--- ERROR GENERAL DEL SCRIPT (FUERA DEL BUCLE PRINCIPAL): {e_global} ---")
        traceback.print_exc()
        take_screenshot(driver, "global_unexpected_error")
    finally:
        logger.info("\n--- Finalizando Script ---")
        save_events()
        logger.warning(f"Total de eventos recolectados en esta sesión guardados en el archivo: {len(scraped_events)}")
        logger.warning(f"Total de IDs únicos recolectados en esta sesión: {len(event_id_counts)}")
        logger.warning(f"Resultados guardados en {output_filename}")
        logger.warning(f"Log completo guardado en {LOG_FILENAME}")

        if driver:
            logger.info("Cerrando navegador...")
            driver.quit()
        # if user_data_dir and os.path.exists(user_data_dir): # Eliminado
        #     logger.info(f"Limpiando directorio temporal de usuario: {user_data_dir}") # Eliminado
        #     try:
        #         shutil.rmtree(user_data_dir)
        #     except OSError as e:
        #         logger.error(f"Error al eliminar directorio temporal {user_data_dir}: {e}")

        logger.info("Script terminado.")

if __name__ == '__main__':
    main()

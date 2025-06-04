# scraper/Dockerfile
FROM python:3.9-slim

# Añade un ARG para "romper" el caché de la capa COPY si su valor cambia.
ARG BUILD_DATE="2025-06-06-FINAL-FIX-V5" # ¡CAMBIA ESTO PARA FORZAR REBUILD!

WORKDIR /app

# Instalar dependencias del sistema y configurar locale.
# Se eliminan las líneas que intentaban cambiar el mirror de apt, confiando en los defaults de Debian.
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    curl \
    wget \
    unzip \
    gnupg \
    ca-certificates \
    iputils-ping \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    libx11-6 \
    libx11-xcb1 \
    libxcb-dri3-0 \
    libxcb1 \
    libxcomposite1 \
    libxdamage1 \
    libxext6 \
    libxfixes3 \
    libxrandr2 \
    libxtst6 \
    libasound2 \
    libcups2 \
    libdbus-1-3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libpangocairo-1.0-0 \
    libgtk-3-0 \
    libgbm1 \
    dos2unix \
    locales \
    && rm -rf /var/lib/apt/lists/* \
    # Configurar locale para Python
    && localedef -i es_CL -c -f UTF-8 -A /usr/share/locale/locale.alias es_CL.UTF-8 \
    && export LANG=es_CL.UTF-8 \
    && export LC_ALL=es_CL.UTF-8

# --- Chrome y ChromeDriver for Testing (CfT) ---
ARG CFT_VERSION="137.0.7151.40"
RUN wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CFT_VERSION}/linux64/chrome-linux64.zip" -O /tmp/chrome-linux64.zip \
    && unzip /tmp/chrome-linux64.zip -d /opt/ \
    && ln -s /opt/chrome-linux64/chrome /usr/bin/google-chrome \
    && rm /tmp/chrome-linux64.zip
RUN wget -q "https://storage.googleapis.com/chrome-for-testing-public/${CFT_VERSION}/linux64/chromedriver-linux64.zip" -O /tmp/chromedriver-linux64.zip \
    && unzip /tmp/chromedriver-linux64.zip -d /opt/ \
    && ln -s /opt/chromedriver-linux64/chromedriver /usr/bin/chromedriver \
    && rm /tmp/chromedriver-linux64.zip \
    && chmod +x /usr/bin/chromedriver \
    && echo "DEBUG: Chrome and ChromeDriver installed."

# Verificar versiones (opcional)
RUN google-chrome --version && chromedriver --version || echo "WARNING: Version check failed."

# Prueba de red (para la fase de build)
RUN echo "DEBUG: Testing network connection from scraper build step..." && ping -c 3 google.com && echo "DEBUG: Network test complete." || echo "WARNING: Network test failed."

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el script scrape_waze.py
COPY scrape_waze.py .

# PASO DE DEPURACIÓN CRÍTICO Y CORRECCIÓN DE INDENTACIÓN AUTOMÁTICA
RUN pip install autopep8 && \
    \
    # Depuración: Antes de cualquier procesamiento
    echo "--- scrape_waze.py: ANTES DE PROCESAR ---" && \
    echo "Líneas: $(wc -l scrape_waze.py)" && \
    echo "Tamaño: $(du -b scrape_waze.py | awk '{print $1}') bytes" && \
    echo "Contenido de líneas ~1060-1080 (cerca de tu final):" && \
    cat -n scrape_waze.py | tail -n +1060 | head -n 30 && \
    echo "-----------------------------------------" && \
    \
    # Procesar con dos2unix
    dos2unix scrape_waze.py && \
    echo "--- scrape_waze.py: DESPUÉS DE dos2unix ---" && \
    echo "Líneas: $(wc -l scrape_waze.py)" && \
    echo "Tamaño: $(du -b scrape_waze.py | awk '{print $1}') bytes" && \
    echo "Contenido de líneas ~1060-1080:" && \
    cat -n scrape_waze.py | tail -n +1060 | head -n 30 && \
    echo "-----------------------------------------" && \
    \
    # Procesar con autopep8
    autopep8 --in-place --aggressive --aggressive scrape_waze.py && \
    echo "--- scrape_waze.py: DESPUÉS DE autopep8 ---" && \
    echo "Líneas: $(wc -l scrape_waze.py)" && \
    echo "Tamaño: $(du -b scrape_waze.py | awk '{print $1}') bytes" && \
    echo "Contenido de líneas ~1060-1080:" && \
    cat -n scrape_waze.py | tail -n +1060 | head -n 30 && \
    echo "-----------------------------------------" && \
    \
    # Procesar con iconv para asegurar codificación ASCII compatible
    iconv -f UTF-8 -t ASCII//TRANSLIT scrape_waze.py -o scrape_waze_ascii.py && \
    mv scrape_waze_ascii.py scrape_waze.py && \
    iconv -f ASCII -t UTF-8 scrape_waze.py -o scrape_waze_utf8.py && \
    mv scrape_waze_utf8.py scrape_waze.py && \
    echo "--- scrape_waze.py: DESPUÉS DE iconv (final) ---" && \
    echo "Líneas: $(wc -l scrape_waze.py)" && \
    echo "Tamaño: $(du -b scrape_waze.py | awk '{print $1}') bytes" && \
    echo "Contenido de líneas ~1060-1080:" && \
    cat -n scrape_waze.py | tail -n +1060 | head -n 30 && \
    echo "----------------------------------------------------------------------------" && \
    echo "--- FIN DEPURACIÓN DE ARCHIVO ---"


# Paso de verificación de sintaxis de Python
RUN echo "DEBUG: Verificando sintaxis de scrape_waze.py con py_compile..." && \
    python -m py_compile scrape_waze.py || (echo "ERROR: Falló la compilación de Python. Revisa la indentación o caracteres invisibles alrededor de la línea indicada." && exit 1) && \
    echo "DEBUG: Sintaxis de scrape_waze.py OK."

RUN echo "BUILD_DATE: $BUILD_DATE"

# CMD final para ejecutar el script principal. Si falla, Docker Compose intentará reiniciar.
CMD ["python", "scrape_waze.py"]

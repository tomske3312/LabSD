# 1. Usar una imagen base de Python
FROM python:3.9-slim

# 2. Establecer el directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Instalar dependencias del sistema: curl, wget, unzip, Chrome/Chromium Y LIBRERÍAS ESENCIALES
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    iputils-ping \
    libnss3 \
    libglib2.0-0 \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# --- PRUEBA DE RED ---
RUN echo ">>> Probando conexión a google.com..." && \
    ping -c 3 google.com && \
    echo ">>> Prueba de ping finalizada."

# --- ChromeDriver ---
# Paso 4a: Descargar ChromeDriver usando curl
RUN echo "Descargando ChromeDriver v135.0.7049.114 con curl..." \
    && CHROMEDRIVER_URL="https://storage.googleapis.com/chrome-for-testing-public/135.0.7049.114/linux64/chromedriver-linux64.zip" \
    && curl -fL -o /tmp/chromedriver-linux64.zip --retry 3 "${CHROMEDRIVER_URL}"

# Paso 4b: Extraer, **Inspeccionar**, Mover y Preparar ChromeDriver
RUN echo "Extrayendo ChromeDriver..." \
    && unzip /tmp/chromedriver-linux64.zip -d /usr/local/bin/ \
    && echo "--- Contenido de /usr/local/bin/ DESPUÉS de unzip ---" \
    && ls -la /usr/local/bin/ \
    && echo "--- Fin del Contenido ---" \
    # Comprobar si el ejecutable está en una subcarpeta (común en Chrome for Testing zips)
    && if [ -f /usr/local/bin/chromedriver-linux64/chromedriver ]; then \
           echo "Moviendo chromedriver desde subcarpeta..."; \
           mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver; \
           rm -rf /usr/local/bin/chromedriver-linux64; \
       else \
           echo "Chromedriver NO encontrado en subcarpeta /usr/local/bin/chromedriver-linux64/"; \
           # Podría estar directamente en /usr/local/bin/chromedriver, o en otra subcarpeta?
       fi \
    && rm /tmp/chromedriver-linux64.zip \
    # Asegurarse de que el archivo exista antes de chmod
    && if [ -f /usr/local/bin/chromedriver ]; then \
            echo "Dando permisos de ejecución a /usr/local/bin/chromedriver..."; \
            chmod +x /usr/local/bin/chromedriver; \
            echo "ChromeDriver listo en /usr/local/bin/chromedriver"; \
       else \
            echo "ERROR: /usr/local/bin/chromedriver NO encontrado después de mover/extraer!"; \
            exit 1; \
       fi

# Paso 4c: Verificar ChromeDriver
RUN echo "Verificando instalación de ChromeDriver..." \
    && chromedriver --version

# Paso 5: Copiar el archivo de requerimientos e instalar dependencias Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Paso 6: Copiar el script de Python al directorio de trabajo
COPY scrape_waze.py .

# Paso 7: Definir el comando por defecto que se ejecutará al iniciar el contenedor
CMD ["python", "scrape_waze.py"]
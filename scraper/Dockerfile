# scraper/Dockerfile (Versión robusta: Chrome estable + ChromeDriver exacto)
FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema y Google Chrome estable
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    unzip \
    gnupg \
    ca-certificates \
    iputils-ping \
    jq \
    libnss3 \
    libglib2.0-0 \
    libgconf-2-4 \
    libfontconfig1 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdbus-1-3 \
    libdrm2 \
    libgtk-3-0 \
    libx11-xcb1 \
    libxss1 \
    libasound2 \
    libxrender1 \
    libxtst6 \
    && wget -q -O - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && sh -c 'echo "deb [arch=amd64] http://dl.google.com/linux/chrome/deb/ stable main" > /etc/apt/sources.list.d/google-chrome.list' \
    && apt-get update \
    && apt-get install -y --no-install-recommends google-chrome-stable \
    && rm -rf /var/lib/apt/lists/*

# Descargar ChromeDriver dinámicamente para que coincida con la versión de Chrome
RUN CHROME_VERSION=$(google-chrome --version | awk '{print $3}') && \
    echo "Chrome version detected: $CHROME_VERSION" && \
    CHROME_MILESTONE=$(echo $CHROME_VERSION | cut -d. -f1) && \
    echo "Chrome milestone: $CHROME_MILESTONE" && \
    CHROMEDRIVER_URL=$(curl -s https://googlechromelabs.github.io/chrome-for-testing/latest-versions-per-milestone-with-downloads.json | jq -r ".milestones[\"$CHROME_MILESTONE\"].downloads.chromedriver[] | select(.platform==\"linux64\") | .url") && \
    echo "Using ChromeDriver URL: $CHROMEDRIVER_URL" && \
    wget -q -O /tmp/chromedriver.zip "$CHROMEDRIVER_URL" && \
    unzip /tmp/chromedriver.zip -d /usr/local/bin/ && \
    if [ -f /usr/local/bin/chromedriver-linux64/chromedriver ]; then \
        mv /usr/local/bin/chromedriver-linux64/chromedriver /usr/local/bin/chromedriver; \
        rm -rf /usr/local/bin/chromedriver-linux64; \
    fi && \
    chmod +x /usr/local/bin/chromedriver && \
    rm /tmp/chromedriver.zip

# Verificar instalación
RUN google-chrome --version && chromedriver --version

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY scrape_waze.py .

CMD ["python", "-u", "/app/scrape_waze.py"]
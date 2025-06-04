# generator/Dockerfile.traffic_generator
FROM python:3.9-slim

# Añade un ARG para "romper" el caché de la capa COPY si su valor cambia.
ARG BUILD_DATE="2025-06-04-FINAL-V4"

WORKDIR /app

# Instala dependencias del sistema necesarias y configura mirrors en una sola capa
RUN apt-get update && \
    # Configurar el mirror más rápido (ej. para Chile) ANTES de instalar paquetes
    echo "deb http://mirror.unsa.edu.ar/debian/ bookworm main contrib non-free" > /etc/apt/sources.list && \
    echo "deb http://mirror.unsa.edu.ar/debian/ bookworm-updates main contrib non-free" >> /etc/apt/sources.list && \
    echo "deb http://mirror.unsa.edu.ar/debian-security/ bookworm-security main contrib non-free" >> /etc/apt/sources.list && \
    # Actualizar de nuevo con los nuevos mirrors y luego instalar paquetes
    apt-get update && \
    apt-get install -y --no-install-recommends \
    wget \
    curl && \
    rm -rf /var/lib/apt/lists/*

COPY requirements_generator.txt .
RUN pip install --no-cache-dir -r requirements_generator.txt

COPY traffic_generator.py .

CMD ["python", "traffic_generator.py"]

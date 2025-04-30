# Usar una imagen base oficial de Python
FROM python:3.10-slim

# Establecer directorio de trabajo
WORKDIR /app

# Copiar archivo de requerimientos e instalar dependencias
COPY requirements_generator.txt .
RUN pip install --no-cache-dir -r requirements_generator.txt

# Copiar el script del generador
COPY traffic_generator.py .

# --- CAMBIO: Descomentar el comando CMD ---
# Comando por defecto para ejecutar el script
CMD ["python", "traffic_generator.py"]

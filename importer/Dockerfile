# importer/Dockerfile
FROM python:3.10-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY import_to_mongo.py .
RUN mkdir -p /app/data

CMD ["python", "-u", "/app/import_to_mongo.py"]
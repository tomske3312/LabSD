# generator/Dockerfile
FROM python:3.9-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY traffic_generator.py .

CMD ["python", "-u", "/app/traffic_generator.py"]
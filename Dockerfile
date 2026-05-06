# Imagen para ejecutar el pipeline ETL (ingesta + staging + processed).
FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY config.py log_config.py ingestion.py staging.py processed.py main.py pytest.ini ./
COPY tests/ tests/
COPY data/ data/

# Por defecto ejecuta el pipeline; sobrescribe el comando si hace falta (ver README).
CMD ["python", "main.py"]

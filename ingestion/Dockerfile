# Image du producteur SkyWatch (OpenSky → Kafka)
FROM python:3.12-slim

# Logs affichés tout de suite (sans tampon) et pas de fichiers .pyc
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# 1) Les dépendances d'abord : Docker garde cette couche en cache
#    tant que requirements.txt ne change pas.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 2) Puis le code
COPY skywatch_ingestion ./skywatch_ingestion

# Ne pas tourner en root dans le conteneur (bonne pratique de sécurité)
RUN useradd --create-home --uid 1000 skywatch
USER skywatch

CMD ["python", "-m", "skywatch_ingestion.flights_producer"]
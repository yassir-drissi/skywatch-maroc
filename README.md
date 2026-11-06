# SkyWatch Maroc

[![CI](https://github.com/yassir-drissi/skywatch-maroc/actions/workflows/ci.yml/badge.svg)](https://github.com/yassir-drissi/skywatch-maroc/actions/workflows/ci.yml)

Plateforme de données temps réel qui suit les avions au-dessus du Maroc,
détecte les situations anormales (attentes en circuit, remises de gaz,
congestion des aéroports) et les croise avec la météo.

**Stack :** Kafka · Spark (Scala) · Databricks · Snowflake · dbt · Airflow · GCP · Docker

## État du projet

| Semaine | Composant | État |
|---|---|---|
| 1 | Ingestion OpenSky + Open-Meteo → Kafka, Docker Compose, tests, CI | ✅ terminé |
| 2 | Spark Structured Streaming (Scala) : parsing, watermark, détection d'anomalies, Bronze | à faire |
| 3 | Databricks : couches Silver et Gold en Delta Lake | à faire |
| 4 | Orchestration Airflow | à faire |
| 5 | Snowflake + dbt | à faire |
| 6 | Dashboard Streamlit, README final, démo vidéo | à faire |

## Architecture actuelle

```
OpenSky API (45 s) ──► flights-producer ──► Kafka : flights.raw   (clé icao24, 3 partitions)
Open-Meteo  (1 h)  ──► weather-producer ──► Kafka : weather.raw   (clé icao aéroport)
                                            Kafka : flights.alerts (rempli par Spark, semaine 2)
```

Tout tourne dans Docker Compose : `kafka`, `kafka-init` (création des topics),
`kafka-ui` (http://localhost:8080), `flights-producer` et `weather-producer`.

## Démarrage rapide

```powershell
Copy-Item .env.example .env      # puis mettre ses identifiants OpenSky
docker compose up -d --build
docker compose logs -f flights-producer
```

Développement local (Python 3.12+) :

```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pip install -r ingestion/requirements.txt
ruff check .
pytest
```

## Topics Kafka

| Topic | Partitions | Rétention | Contenu | Schéma |
|---|---|---|---|---|
| `flights.raw` | 3 | 7 jours | une position d'avion par message | `ingestion/skywatch_ingestion/schemas/flights_raw.schema.json` |
| `weather.raw` | 1 | 30 jours | météo actuelle d'un aéroport | `ingestion/skywatch_ingestion/schemas/weather_raw.schema.json` |
| `flights.alerts` | 1 | 30 jours | alertes (attentes, remises de gaz) | semaine 2 |
# SkyWatch Maroc

[![CI](https://github.com/yassir-drissi/skywatch-maroc/actions/workflows/ci.yml/badge.svg)](https://github.com/yassir-drissi/skywatch-maroc/actions/workflows/ci.yml)

Plateforme de données temps réel qui suit les avions au-dessus du Maroc,
détecte les situations anormales (attentes en circuit, remises de gaz,
congestion des aéroports) et les croise avec la météo.

**Stack :** Kafka · Spark Structured Streaming (Scala) · Databricks · Snowflake · dbt · Airflow · GCP · Docker

## État du projet

| Semaine | Composant | État |
|---|---|---|
| 1 | Ingestion OpenSky + Open-Meteo → Kafka, Docker Compose, tests, CI | ✅ terminé |
| 2 | Spark Structured Streaming (Scala) : Bronze, watermark, trafic, attentes en circuit, remises de gaz | ✅ terminé |
| 3 | Databricks : couches Silver et Gold en Delta Lake | à faire |
| 4 | Orchestration Airflow | à faire |
| 5 | Snowflake + dbt | à faire |
| 6 | Dashboard Streamlit, README final, démo vidéo | à faire |

## Architecture actuelle

```
OpenSky (45 s) ─► flights-producer ─► Kafka flights.raw ─┐
Open-Meteo (1 h) ─► weather-producer ─► Kafka weather.raw ─┤
                                                           ▼
                                    spark-streaming (StreamingApp, Scala)
                                      ├─ bronze_flights   ─► data/lake/bronze/flights   (Parquet)
                                      ├─ bronze_weather   ─► data/lake/bronze/weather   (Parquet)
                                      ├─ traffic_5min     ─► trafic par aéroport (console)
                                      ├─ holding_alerts   ─► Kafka flights.alerts (HOLDING)
                                      └─ go_around_alerts ─► Kafka flights.alerts (GO_AROUND)
```

Services Docker Compose : `kafka`, `kafka-init` (création des topics), `kafka-ui`
(http://localhost:8080), `flights-producer`, `weather-producer`, `spark-streaming`
(Spark UI : http://localhost:4040).

## Démarrage rapide (Windows · PowerShell)

```powershell
Copy-Item .env.example .env          # puis mettre ses identifiants OpenSky
sbt streaming/package                # construit le jar du job Spark
docker compose up -d --build
docker compose logs -f spark-streaming
```

Démontrer les alertes sans attendre un vrai cas (messages marqués `source = "demo"`) :

```powershell
$env:PYTHONPATH = "ingestion"
python -m skywatch_ingestion.demo_holding     # → alerte HOLDING dans flights.alerts
python -m skywatch_ingestion.demo_go_around   # → alerte GO_AROUND dans flights.alerts
```

## Développement

```powershell
# Python (ingestion)
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pip install -r ingestion/requirements.txt
ruff check .
pytest

# Scala (streaming) : Java 17+ et sbt
sbt streaming/test
```

## Topics Kafka

| Topic | Partitions | Rétention | Contenu | Schéma |
|---|---|---|---|---|
| `flights.raw` | 3 | 7 jours | une position d'avion par message (clé `icao24`) | `ingestion/skywatch_ingestion/schemas/flights_raw.schema.json` |
| `weather.raw` | 1 | 30 jours | météo actuelle d'un aéroport (clé `icao`) | `ingestion/skywatch_ingestion/schemas/weather_raw.schema.json` |
| `flights.alerts` | 1 | 30 jours | anomalie détectée par Spark (clé `icao24`) | `ingestion/skywatch_ingestion/schemas/flights_alerts.schema.json` |

## Limites connues

- **Couverture OpenSky** : près des aéroports, seules les approches de **Tanger (GMTT)** sont visibles
  à basse altitude. Casablanca, Rabat et Fès n'ont que des survols à 9 000–11 000 m. Les attentes et
  remises de gaz ne sont donc détectables, en pratique, qu'à Tanger.
- **Pas de retards** : OpenSky ne donne pas les horaires prévus des vols.
- **Bronze en local** (`data/lake`) : le passage vers GCS est prévu en semaine 3 (Databricks).

Détails, décisions et suite du travail : voir [HANDOFF.md](HANDOFF.md).
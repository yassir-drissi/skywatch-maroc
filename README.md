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
| 3 | Lakehouse Silver / Gold en Delta Lake (PySpark), local + job Databricks planifié | ✅ terminé |
| 4 | Orchestration Airflow | à faire |
| 5 | Snowflake + dbt | à faire |
| 6 | Dashboard Streamlit, README final, démo vidéo | à faire |

## Architecture

```
OpenSky (45 s) ─► flights-producer ─► Kafka flights.raw ─┐
Open-Meteo (1 h) ─► weather-producer ─► Kafka weather.raw ─┤
                                                           ▼
                                    spark-streaming (StreamingApp, Scala)
                                      ├─ bronze_flights   ─► data/lake/bronze/flights   (Parquet)
                                      ├─ bronze_weather   ─► data/lake/bronze/weather   (Parquet)
                                      ├─ bronze_alerts    ─► data/lake/bronze/alerts    (Parquet)
                                      ├─ traffic_5min     ─► trafic par aéroport (console)
                                      ├─ holding_alerts   ─► Kafka flights.alerts (HOLDING)
                                      └─ go_around_alerts ─► Kafka flights.alerts (GO_AROUND)
                                                    │
                     data/lake/bronze  ─────────────┘   (local, ou copié vers un volume Databricks)
                            │
                            ▼   semaine 3 : PySpark + Delta Lake (dossier databricks/)
                     run_silver.py ─► silver_flights, silver_weather            (MERGE)
                            │
                            ▼   contrôles qualité avant écriture
                     run_gold.py   ─► gold_flights          un vol par ligne : arrivée / départ / survol
                                     gold_airport_hourly   trafic par aéroport et par heure × météo
                                     gold_anomalies        alertes reliées au vol et à la météo
```

Le même code Silver / Gold tourne **en local** (Docker, dossiers Delta dans `data/lake`) et sur
**Databricks Free Edition** (job planifié `silver → gold`, tables Unity Catalog `workspace.skywatch.*`).

Services Docker Compose : `kafka`, `kafka-init` (création des topics), `kafka-ui`
(http://localhost:8080), `flights-producer`, `weather-producer`, `spark-streaming`
(Spark UI : http://localhost:4040), et `lakehouse` (profil `batch`, lancé à la demande pour Silver / Gold).

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

### Silver et Gold (semaine 3)

```powershell
docker compose --profile batch run --rm lakehouse jobs/run_silver.py
docker compose --profile batch run --rm lakehouse jobs/run_gold.py
```

Sur Databricks : envoyer Bronze avec `.\scripts\sync_bronze_databricks.ps1`, puis lancer le job
`skywatch-lakehouse`. Mise en place pas à pas : [HANDOFF.md](HANDOFF.md), partie A6.

```sql
SELECT flight_type, airport, COUNT(*) AS vols
FROM workspace.skywatch.gold_flights GROUP BY ALL ORDER BY vols DESC;
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

# Lakehouse (Silver / Gold) : tests dans l'image Spark (PySpark marche mal directement sous Windows)
docker run --rm -u 0 -v "${PWD}:/app" -w /app `
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app/databricks:/app/ingestion:/app/tests/lakehouse `
  -e PYSPARK_PYTHON=python3 spark:3.5.7-scala2.12-java17-python3-ubuntu `
  bash -c "pip install -q pytest==8.3.4 && python3 -m pytest tests/lakehouse -q -p no:cacheprovider"
```

Tests : 55 Python (ingestion) · 16 lakehouse (vrai Spark local) · 39 Scala. La CI lance les trois
(jobs `python`, `lakehouse`, `scala`).

## Topics Kafka

| Topic | Partitions | Rétention | Contenu | Schéma |
|---|---|---|---|---|
| `flights.raw` | 3 | 7 jours | une position d'avion par message (clé `icao24`) | `ingestion/skywatch_ingestion/schemas/flights_raw.schema.json` |
| `weather.raw` | 1 | 30 jours | météo actuelle d'un aéroport (clé `icao`) | `ingestion/skywatch_ingestion/schemas/weather_raw.schema.json` |
| `flights.alerts` | 1 | 30 jours | anomalie détectée par Spark (clé `icao24`) | `ingestion/skywatch_ingestion/schemas/flights_alerts.schema.json` |

## Limites connues

- **Couverture OpenSky** : près des aéroports, seules les approches de **Tanger (GMTT)** sont visibles
  à basse altitude. Casablanca, Rabat et Fès n'ont que des survols à 9 000–11 000 m. Les attentes et
  remises de gaz ne sont donc détectables, en pratique, qu'à Tanger. Gold le mesure : le 9 novembre,
  sur 283 vols reconstruits, 275 étaient des survols et les 4 arrivées et 3 départs étaient tous à Tanger.
- **Pas de retards** : OpenSky ne donne pas les horaires prévus des vols.
- **Bronze en local** (`data/lake`) : Kafka et le streaming tournent dans Docker sur le PC. Databricks
  ne voit que ce qu'on y envoie avec `scripts/sync_bronze_databricks.ps1` (pas de flux continu vers le cloud).

Détails, décisions et suite du travail : voir [HANDOFF.md](HANDOFF.md).
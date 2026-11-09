"""Job Silver -> Gold (Delta), avec contrôles qualité.

En local (Docker) :
    docker compose --profile batch run --rm lakehouse jobs/run_gold.py
Sur Databricks (tâche "Python script", après run_silver) :
    paramètres : --lake-path /Volumes/skywatch/lakehouse/lake --schema skywatch.lakehouse
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path

# Permet "import skywatch_lakehouse" quel que soit le dossier de lancement (local ou Databricks)
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pyspark.sql import SparkSession  # noqa: E402
from skywatch_lakehouse import quality  # noqa: E402
from skywatch_lakehouse.gold import airport_hourly, anomalies_to_gold, flights_to_gold  # noqa: E402
from skywatch_lakehouse.lake import Lake, cache  # noqa: E402

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("run_gold")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SkyWatch : Silver -> Gold")
    p.add_argument("--lake-path", default=os.getenv("LAKE_PATH", "data/lake"))
    p.add_argument("--schema", default=os.getenv("SKYWATCH_SCHEMA") or None,
                   help="catalog.schema Unity Catalog (Databricks). Vide = dossiers Delta locaux.")
    return p.parse_args(argv)


def read_alerts(spark: SparkSession, lake: Lake):
    try:
        return lake.read_bronze(spark, "alerts")
    except Exception:  # noqa: BLE001  (query bronze_alerts pas encore lancée)
        log.warning("Pas de bronze/alerts : gold_anomalies ignorée")
        return None


def run(spark: SparkSession, lake: Lake) -> None:
    silver_flights = cache(lake.read(spark, "silver", "flights"))
    silver_weather = cache(lake.read(spark, "silver", "weather"))

    # 1. Calculer les tables Gold
    flights = cache(flights_to_gold(silver_flights))
    hourly = cache(airport_hourly(silver_flights, flights, silver_weather))
    bronze_alerts = read_alerts(spark, lake)
    anomalies = None
    if bronze_alerts is not None:
        anomalies = cache(anomalies_to_gold(bronze_alerts, flights, silver_weather))
        if not anomalies.take(1):
            anomalies = None

    # 2. Contrôles qualité AVANT d'écrire : si un contrôle échoue, rien n'est publié
    problems = quality.check_silver_flights(silver_flights)
    problems += quality.check_gold_flights(flights)
    problems += quality.check_airport_hourly(hourly)
    if anomalies is not None:
        problems += quality.check_anomalies(anomalies)
    if problems:
        for p in problems:
            log.error("Qualité KO | %s", p)
        raise RuntimeError(f"{len(problems)} contrôle(s) qualité en échec : Gold non mis à jour")
    log.info("Qualité OK (%d positions Silver contrôlées)", silver_flights.count())

    # 3. Écrire
    lake.upsert(spark, flights, "gold", "flights", keys=["flight_id", "flight_date"], partition_by=["flight_date"])
    log.info("Gold vols : %d vols", flights.count())
    for r in flights.groupBy("flight_type", "airport").count().orderBy("flight_type", "airport").collect():
        log.info("  %-10s %-5s : %d", r["flight_type"], r["airport"] or "-", r["count"])

    lake.overwrite(hourly, "gold", "airport_hourly")   # petit agrégat : recalculé entièrement
    log.info("Gold aéroport x heure : %d lignes", hourly.count())

    if anomalies is not None:
        lake.upsert(spark, anomalies, "gold", "anomalies",
                    keys=["alert_type", "icao24", "detected_at"], partition_by=["date"])
        for r in anomalies.groupBy("alert_type", "is_demo").count().collect():
            log.info("  alertes %-9s démo=%s : %d", r["alert_type"], r["is_demo"], r["count"])


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = (SparkSession.builder.appName("SkyWatch-Gold")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    lake = Lake(args.lake_path, args.schema)
    log.info("Lake : %s | tables : %s", lake.lake_path, lake.schema or "dossiers Delta")
    run(spark, lake)
    log.info("Gold OK")


if __name__ == "__main__":
    main()

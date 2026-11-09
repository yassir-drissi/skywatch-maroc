"""Job Bronze -> Silver (Delta).

En local (Docker) :
    docker compose --profile batch run --rm lakehouse jobs/run_silver.py
Sur Databricks (tâche "Python script") :
    paramètres : --lake-path /Volumes/skywatch/lakehouse/lake --schema skywatch.lakehouse
"""

from __future__ import annotations

import argparse
import logging
import os
import sys
from pathlib import Path


def _lakehouse_dir() -> Path:
    """Dossier databricks/ (celui qui contient skywatch_lakehouse), en local comme sur Databricks.

    Databricks serverless lance le script avec exec() : __file__ n'existe pas.
    On essaie donc __file__, puis le chemin du script (sys.argv[0]), puis le dossier courant.
    """
    candidates = []
    try:
        candidates.append(Path(__file__).resolve().parents[1])
    except NameError:
        pass
    if sys.argv and sys.argv[0]:
        candidates.append(Path(sys.argv[0]).resolve().parent.parent)
    candidates += [Path.cwd().parent, Path.cwd()]
    for folder in candidates:
        if (folder / "skywatch_lakehouse").is_dir():
            return folder
    raise RuntimeError(f"skywatch_lakehouse introuvable (cherché dans {candidates})")


# Permet "import skywatch_lakehouse" quel que soit le dossier de lancement (local ou Databricks)
sys.path.insert(0, str(_lakehouse_dir()))

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F  # noqa: E402
from skywatch_lakehouse.lake import Lake, cache, uncache  # noqa: E402
from skywatch_lakehouse.silver import (  # noqa: E402
    flights_quality_report,
    flights_to_silver,
    weather_to_silver,
)

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s - %(message)s")
log = logging.getLogger("run_silver")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(description="SkyWatch : Bronze -> Silver")
    p.add_argument("--lake-path", default=os.getenv("LAKE_PATH", "data/lake"))
    p.add_argument("--schema", default=os.getenv("SKYWATCH_SCHEMA") or None,
                   help="catalog.schema Unity Catalog (Databricks). Vide = dossiers Delta locaux.")
    p.add_argument("--since-days", type=int, default=None,
                   help="Ne relire que les N derniers jours de Bronze (défaut : tout)")
    return p.parse_args(argv)


def run(spark: SparkSession, lake: Lake, since_days: int | None = None) -> None:
    # --- Vols ---------------------------------------------------------------
    bronze = lake.read_bronze(spark, "flights", since_days)
    report = flights_quality_report(bronze)
    log.info("Bronze vols : %(bronze)d messages | démo écartés : %(demo)d | "
             "positions invalides : %(invalid_position)d", report)

    silver = cache(flights_to_silver(bronze))
    n = silver.count()
    if n == 0:
        raise RuntimeError("Silver vols vide : le streaming a-t-il écrit dans bronze/flights ?")
    lake.upsert(spark, silver, "silver", "flights",
                keys=["icao24", "snapshot_time", "event_date"], partition_by=["event_date"])
    log.info("Silver vols : %d positions (%d doublons enlevés)",
             n, report["opensky"] - report["invalid_position"] - n)

    by_airport = (silver.filter("in_terminal_area")
                  .groupBy("nearest_airport").agg(F.countDistinct("icao24").alias("avions"))
                  .orderBy(F.desc("avions")).collect())
    for r in by_airport:
        log.info("  zone %s : %d avions différents", r["nearest_airport"], r["avions"])
    uncache(silver)

    # --- Météo --------------------------------------------------------------
    try:
        bronze_weather = lake.read_bronze(spark, "weather", since_days)
    except Exception:  # noqa: BLE001  (pas encore de météo dans Bronze)
        log.warning("Pas de bronze/weather : météo ignorée")
        return
    weather = weather_to_silver(bronze_weather)
    lake.upsert(spark, weather, "silver", "weather",
                keys=["icao", "observed_at"], partition_by=["observed_date"])
    log.info("Silver météo : %d relevés", weather.count())


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    spark = (SparkSession.builder.appName("SkyWatch-Silver")
             .config("spark.sql.session.timeZone", "UTC")
             .getOrCreate())
    lake = Lake(args.lake_path, args.schema)
    log.info("Lake : %s | tables : %s", lake.lake_path, lake.schema or "dossiers Delta")
    run(spark, lake, args.since_days)
    log.info("Silver OK")


if __name__ == "__main__":
    main()
"""Test du job run_silver de bout en bout : faux Bronze Parquet -> Silver.

Delta n'est pas installé pour les tests : un faux Lake écrit en Parquet et simule le MERGE
(union + garder la ligne la plus récente). On vérifie surtout le câblage et l'idempotence.
"""

from __future__ import annotations

import importlib.util
import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from skywatch_lakehouse.lake import Lake  # noqa: E402

ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = 1762430400


def load_job():
    spec = importlib.util.spec_from_file_location("run_silver", ROOT / "databricks/jobs/run_silver.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


class ParquetLake(Lake):
    """Comme Lake, mais en Parquet (pas besoin des jars Delta)."""

    def exists(self, spark, layer, table):
        return Path(self.name(layer, table)).exists()

    def read(self, spark, layer, table):
        return spark.read.parquet(self.name(layer, table))

    def upsert(self, spark, df, layer, table, keys, partition_by=None):
        if self.exists(spark, layer, table):
            old = self.read(spark, layer, table)
            # MERGE simulé : on garde les anciennes lignes dont la clé n'est pas dans le nouveau lot
            df = old.join(df.select(*keys), keys, "left_anti").unionByName(df).localCheckpoint()
        df.write.mode("overwrite").parquet(self.name(layer, table))


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("tests-run-silver")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def write_bronze(spark, path: Path, messages: list[dict]) -> None:
    rows = [(json.dumps(m), datetime(2025, 11, 6, 12, 0, i), date(2025, 11, 6)) for i, m in enumerate(messages)]
    (spark.createDataFrame(rows, "raw_json string, kafka_timestamp timestamp, ingest_date date")
     .write.mode("overwrite").parquet(str(path)))


def test_run_silver_end_to_end_and_idempotent(spark, tmp_path):
    base = {"schema_version": 1, "source": "opensky", "snapshot_time": SNAPSHOT,
            "ingested_at": "2025-11-06T12:00:05Z", "last_contact": SNAPSHOT, "on_ground": False,
            "latitude": 35.70, "longitude": -5.95, "baro_altitude": 621.0}
    write_bronze(spark, tmp_path / "bronze/flights", [
        {**base, "icao24": "020123"},
        {**base, "icao24": "020123"},                     # doublon
        {**base, "icao24": "3c6444", "latitude": 33.4, "longitude": -7.6},
        {**base, "icao24": "abcdef", "source": "demo"},   # démo
    ])
    weather = {"schema_version": 1, "source": "open-meteo", "icao": "GMTT", "observed_at": SNAPSHOT,
               "ingested_at": "2025-11-06T12:00:05Z", "visibility_m": 10000.0}
    write_bronze(spark, tmp_path / "bronze/weather", [weather])

    job = load_job()
    lake = ParquetLake(str(tmp_path))
    job.run(spark, lake)
    job.run(spark, lake)   # 2e passage : ne doit rien dupliquer

    flights = lake.read(spark, "silver", "flights")
    assert flights.count() == 2
    assert {r.nearest_airport for r in flights.collect()} == {"GMTT", "GMMN"}
    assert lake.read(spark, "silver", "weather").count() == 1


def test_run_silver_fails_loudly_when_bronze_has_no_real_flights(spark, tmp_path):
    write_bronze(spark, tmp_path / "bronze/flights",
                 [{"source": "demo", "icao24": "abcdef", "snapshot_time": SNAPSHOT}])
    with pytest.raises(RuntimeError, match="Silver vols vide"):
        load_job().run(spark, ParquetLake(str(tmp_path)))

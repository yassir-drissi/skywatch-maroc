"""Tests de Silver -> Gold et du job run_gold (Spark local, sans Delta)."""

from __future__ import annotations

import json
import os
import sys
from datetime import date, datetime
from pathlib import Path

import pytest

pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from skywatch_lakehouse import quality  # noqa: E402
from skywatch_lakehouse.gold import airport_hourly, anomalies_to_gold, flights_to_gold  # noqa: E402
from skywatch_lakehouse.silver import flights_to_silver, weather_to_silver  # noqa: E402

T0 = 1762430400  # 2025-11-06 12:00:00 UTC
GMTT = (35.7269, -5.9169)
GMMN = (33.3675, -7.5900)
GMME = (34.0515, -6.7515)


@pytest.fixture(scope="module")
def spark():
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("tests-gold")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def pos(icao24, t, lat, lon, alt, callsign="RAM200", **extra):
    return {"schema_version": 1, "source": "opensky", "snapshot_time": t, "ingested_at": "2025-11-06T12:00:00Z",
            "icao24": icao24, "callsign": callsign, "origin_country": "Morocco", "last_contact": t,
            "on_ground": False, "latitude": lat, "longitude": lon, "baro_altitude": alt, "velocity": 80.0, **extra}


def to_bronze(spark, messages):
    rows = [(json.dumps(m), datetime(2025, 11, 6, 12, 0, 0), date(2025, 11, 6)) for m in messages]
    return spark.createDataFrame(rows, "raw_json string, kafka_timestamp timestamp, ingest_date date")


def scenario():
    msgs = []
    # Arrivée à Tanger : descend de 3000 m à 100 m en s'approchant (une position par minute)
    for i in range(10):
        msgs.append(pos("020123", T0 + 60 * i, GMTT[0] + 0.25 - 0.025 * i, GMTT[1], 3000 - 320 * i))
    # Même avion 2 h plus tard : décolle de Tanger -> 2e vol (trou > 30 min)
    for i in range(5):
        msgs.append(pos("020123", T0 + 7200 + 60 * i, GMTT[0] + 0.02 * i, GMTT[1], 100 + 500 * i, callsign="RAM201"))
    # Départ de Casablanca
    for i in range(6):
        msgs.append(pos("3c6444", T0 + 60 * i, GMMN[0] + 0.03 * i, GMMN[1], 300 + 400 * i, callsign="AFR1234"))
    # Survol de Rabat à 11 000 m
    for i in range(4):
        msgs.append(pos("4ca123", T0 + 60 * i, GMME[0], GMME[1] + 0.05 * i, 11000, callsign="RYR55"))
    return msgs


def weather(spark):
    w = {"schema_version": 1, "source": "open-meteo", "icao": "GMTT", "observed_at": T0,
         "ingested_at": "2025-11-06T12:00:00Z", "visibility_m": 2000.0, "wind_speed_kt": 25.0,
         "wind_gust_kt": 35.0, "temperature_c": 18.0, "precipitation_mm": 1.2}
    return weather_to_silver(to_bronze(spark, [w, {**w, "observed_at": T0 + 900, "visibility_m": 4000.0}]))


@pytest.fixture(scope="module")
def silver(spark):
    return flights_to_silver(to_bronze(spark, scenario())).cache()


@pytest.fixture(scope="module")
def gold_flights(silver):
    return flights_to_gold(silver).cache()


def test_flights_are_split_and_classified(gold_flights):
    rows = {r.flight_id: r for r in gold_flights.collect()}
    assert len(rows) == 4
    arrival = rows[f"020123_{T0}"]
    assert arrival.flight_type == "ARRIVAL"
    assert arrival.airport == "GMTT"
    assert arrival.callsign == "RAM200"
    assert arrival.positions == 10
    assert arrival.lowest_agl_m == 120 - 21      # dernier point : 120 m, piste de Tanger à 21 m
    assert rows[f"020123_{T0 + 7200}"].flight_type == "DEPARTURE"
    assert rows[f"020123_{T0 + 7200}"].callsign == "RAM201"
    assert rows[f"3c6444_{T0}"].flight_type == "DEPARTURE"
    assert rows[f"3c6444_{T0}"].airport == "GMMN"
    overflight = rows[f"4ca123_{T0}"]
    assert overflight.flight_type == "OVERFLIGHT"
    assert overflight.airport is None


def test_airport_hourly_counts_and_weather(spark, silver, gold_flights):
    rows = {(r.airport, r.hour): r for r in airport_hourly(silver, gold_flights, weather(spark)).collect()}
    noon = rows[("GMTT", 12)]
    assert noon.arrivals == 1
    assert noon.departures == 0
    assert noon.aircraft == 1
    assert noon.min_visibility_m == 2000.0        # le pire relevé de l'heure
    assert noon.max_gust_kt == 35.0
    assert noon.low_visibility is True
    assert rows[("GMTT", 14)].departures == 1
    assert rows[("GMMN", 12)].departures == 1
    assert rows[("GMME", 12)].low_level_aircraft == 0   # survol à 11 000 m


def test_anomalies_linked_to_flight_and_weather(spark, gold_flights):
    alerts = [
        {"schema_version": 1, "alert_type": "GO_AROUND", "icao24": "020123", "airport": "GMTT",
         "detected_at": T0 + 300, "latitude": 35.8, "longitude": -5.9, "positions": 6,
         "lowest_at": T0 + 240, "lowest_altitude_m": 400.0, "lowest_agl_m": 379.0, "climb_m": 250.0},
        {"schema_version": 1, "alert_type": "HOLDING", "icao24": "fff001", "callsign": "DEMO01", "airport": "GMMN",
         "detected_at": T0, "latitude": 33.4, "longitude": -7.6, "positions": 12,
         "window_start": T0 - 600, "total_turn_deg": 380.0},
    ]
    bronze = to_bronze(spark, alerts + alerts[:1])   # + un doublon
    rows = {r.alert_type: r for r in anomalies_to_gold(bronze, gold_flights, weather(spark)).collect()}
    assert len(rows) == 2
    go = rows["GO_AROUND"]
    assert go.flight_id == f"020123_{T0}"
    assert go.callsign == "RAM200"                  # repris du vol
    assert go.is_demo is False
    assert go.low_visibility is True
    assert rows["HOLDING"].is_demo is True


def test_quality_checks_pass_on_good_data(silver, gold_flights):
    assert quality.check_silver_flights(silver) == []
    assert quality.check_gold_flights(gold_flights) == []


def test_quality_checks_catch_duplicates(silver, gold_flights):
    assert quality.check_gold_flights(gold_flights.unionByName(gold_flights))
    assert quality.check_silver_flights(silver.unionByName(silver))


def test_run_gold_end_to_end(spark, tmp_path, silver):
    import importlib.util

    from test_run_silver import ParquetLake

    class Lake2(ParquetLake):
        def overwrite(self, df, layer, table, partition_by=None):
            df.write.mode("overwrite").parquet(self.name(layer, table))

    lake = Lake2(str(tmp_path))
    silver.write.parquet(lake.name("silver", "flights"))
    weather(spark).write.parquet(lake.name("silver", "weather"))

    root = Path(__file__).resolve().parents[2]
    spec = importlib.util.spec_from_file_location("run_gold", root / "databricks/jobs/run_gold.py")
    job = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(job)
    job.run(spark, lake)   # pas de bronze/alerts : doit continuer sans anomalies
    job.run(spark, lake)   # 2e passage : idempotent

    assert lake.read(spark, "gold", "flights").count() == 4
    assert lake.read(spark, "gold", "airport_hourly").count() >= 4
    assert not Path(lake.name("gold", "anomalies")).exists()

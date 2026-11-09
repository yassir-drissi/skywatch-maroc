"""Tests de Bronze -> Silver, avec un vrai Spark local (sans Delta, sans Kafka)."""

from __future__ import annotations

import json
import os
import re
import sys
from datetime import datetime
from pathlib import Path

import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.sql import SparkSession  # noqa: E402
from skywatch_lakehouse.airports import AIRPORTS  # noqa: E402
from skywatch_lakehouse.silver import (  # noqa: E402
    flights_quality_report,
    flights_to_silver,
    weather_to_silver,
)

ROOT = Path(__file__).resolve().parents[2]
SNAPSHOT = 1762430400  # 2025-11-06 12:00:00 UTC


@pytest.fixture(scope="module")
def spark():
    # Les workers Spark doivent utiliser le même Python que pytest (sinon PYTHON_VERSION_MISMATCH)
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    s = (SparkSession.builder.master("local[1]").appName("tests-silver")
         .config("spark.sql.session.timeZone", "UTC")
         .config("spark.sql.shuffle.partitions", "1")
         .config("spark.ui.enabled", "false")
         .getOrCreate())
    yield s
    s.stop()


def flight(**over):
    msg = {"schema_version": 1, "source": "opensky", "snapshot_time": SNAPSHOT,
           "ingested_at": "2025-11-06T12:00:05Z", "icao24": "020123", "callsign": "RAM200  ",
           "origin_country": "Morocco", "last_contact": SNAPSHOT, "on_ground": False,
           "latitude": 35.70, "longitude": -5.95, "baro_altitude": 621.0, "velocity": 70.0}
    msg.update(over)
    return msg


def bronze(spark, messages, ts=None):
    rows = [(json.dumps(m), ts or datetime(2025, 11, 6, 12, 0, 6)) for m in messages]
    return spark.createDataFrame(rows, "raw_json string, kafka_timestamp timestamp")


def test_flight_is_cleaned_and_enriched(spark):
    row = flights_to_silver(bronze(spark, [flight()])).first()
    assert row.icao24 == "020123"
    assert row.callsign == "RAM200"
    assert row.airline_icao == "RAM"
    assert str(row.event_date) == "2025-11-06"   # (event_time est affiché par Python dans le fuseau du PC)
    assert row.event_hour == 12
    assert row.velocity_kmh == 252.0
    assert row.nearest_airport == "GMTT"          # à ~3 km de Tanger
    assert row.distance_km < 5
    assert row.agl_m == 600                       # 621 m - 21 m (piste de Tanger)
    assert row.in_terminal_area is True


def test_demo_invalid_and_out_of_zone_are_dropped(spark):
    msgs = [
        flight(),
        flight(source="demo", icao24="abcdef"),
        flight(icao24="xyz"),                     # identifiant invalide
        flight(icao24="020124", latitude=None),   # pas de position
        flight(icao24="020125", latitude=40.0),   # hors zone (Espagne)
    ]
    df = bronze(spark, msgs)
    assert flights_to_silver(df).count() == 1
    assert flights_quality_report(df) == {"bronze": 5, "demo": 1, "opensky": 4, "invalid_position": 3}


def test_duplicates_keep_latest(spark):
    old = bronze(spark, [flight(baro_altitude=600.0)], datetime(2025, 11, 6, 12, 0, 6))
    new = bronze(spark, [flight(baro_altitude=650.0)], datetime(2025, 11, 6, 12, 5, 0))
    out = flights_to_silver(old.unionByName(new)).collect()
    assert len(out) == 1
    assert out[0].altitude_m == 650.0


def test_empty_callsign_and_aberrant_altitude_become_null(spark):
    row = flights_to_silver(bronze(spark, [flight(callsign="   ", baro_altitude=99999.0,
                                                  geo_altitude=None)])).first()
    assert row.callsign is None
    assert row.airline_icao is None
    assert row.altitude_m is None
    assert row.agl_m is None


def test_geo_altitude_used_when_baro_missing(spark):
    row = flights_to_silver(bronze(spark, [flight(baro_altitude=None, geo_altitude=700.0)])).first()
    assert row.altitude_m == 700.0


def test_far_from_airports_not_in_terminal_area(spark):
    row = flights_to_silver(bronze(spark, [flight(latitude=29.0, longitude=-3.0)])).first()
    assert row.in_terminal_area is False
    assert row.distance_km > 60


def test_weather_to_silver(spark):
    w = {"schema_version": 1, "source": "open-meteo", "icao": "GMMN", "observed_at": SNAPSHOT,
         "ingested_at": "2025-11-06T12:00:05Z", "visibility_m": 3000.0, "wind_speed_kt": 12.0,
         "wind_gust_kt": 31.0, "temperature_c": 21.5}
    df = bronze(spark, [w, w, {**w, "source": "demo"}, {**w, "icao": "LFPG"}])
    rows = weather_to_silver(df).collect()
    assert len(rows) == 1
    assert rows[0].low_visibility is True
    assert rows[0].strong_wind is True          # rafales >= 30 kt
    assert rows[0].observed_hour == 12


def test_airports_match_scala():
    """Les élévations doivent rester identiques à Airports.scala (sinon AGL différent)."""
    scala = next(ROOT.glob("streaming/**/Airports.scala"), None)
    if scala is None:
        pytest.skip("Airports.scala introuvable")
    text = scala.read_text(encoding="utf-8")
    for a in AIRPORTS:
        assert a.icao in text
        line = next(li for li in text.splitlines() if f'"{a.icao}"' in li)
        numbers = [float(n) for n in re.findall(r"-?\d+(?:\.\d+)?", line)]
        assert a.lat in numbers and a.lon in numbers and a.elevation_m in numbers, line

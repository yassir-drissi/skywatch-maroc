"""Contrôles qualité : le job échoue (et ne publie rien de faux) si une règle est violée.

Chaque fonction renvoie la liste des problèmes trouvés (vide = OK).
"""

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

FLIGHT_TYPES = ("ARRIVAL", "DEPARTURE", "LOW_LEVEL", "OVERFLIGHT")


def unique(df: DataFrame, keys: list[str], name: str) -> list[str]:
    dups = df.groupBy(*keys).count().filter("count > 1").count()
    return [f"{name} : {dups} clés {keys} en double"] if dups else []


def not_null(df: DataFrame, cols: list[str], name: str) -> list[str]:
    row = df.agg(*[F.sum(F.col(c).isNull().cast("int")).alias(c) for c in cols]).first()
    return [f"{name} : {row[c]} valeurs nulles dans {c}" for c in cols if row[c]]


def check_silver_flights(df: DataFrame) -> list[str]:
    problems = unique(df, ["icao24", "snapshot_time"], "silver_flights")
    problems += not_null(df, ["icao24", "snapshot_time", "event_time", "latitude", "longitude"], "silver_flights")
    demo = df.filter(F.col("icao24").isin("fff001", "fff002")).count()
    if demo:
        problems.append(f"silver_flights : {demo} positions de démo")
    return problems


def check_gold_flights(df: DataFrame) -> list[str]:
    problems = unique(df, ["flight_id"], "gold_flights")
    problems += not_null(df, ["flight_id", "icao24", "flight_type", "first_seen"], "gold_flights")
    bad_type = df.filter(~F.col("flight_type").isin(*FLIGHT_TYPES)).count()
    if bad_type:
        problems.append(f"gold_flights : {bad_type} flight_type inconnus")
    no_airport = df.filter(F.col("flight_type").isin("ARRIVAL", "DEPARTURE") & F.col("airport").isNull()).count()
    if no_airport:
        problems.append(f"gold_flights : {no_airport} arrivées/départs sans aéroport")
    backwards = df.filter(F.col("last_seen") < F.col("first_seen")).count()
    if backwards:
        problems.append(f"gold_flights : {backwards} vols qui finissent avant de commencer")
    return problems


def check_airport_hourly(df: DataFrame) -> list[str]:
    problems = unique(df, ["airport", "date", "hour"], "gold_airport_hourly")
    bad_hour = df.filter(~F.col("hour").between(0, 23)).count()
    if bad_hour:
        problems.append(f"gold_airport_hourly : {bad_hour} heures hors 0-23")
    return problems


def check_anomalies(df: DataFrame) -> list[str]:
    return unique(df, ["alert_type", "icao24", "detected_at"], "gold_anomalies")

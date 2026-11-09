"""Silver -> Gold : les tables qu'on interroge pour répondre aux questions métier.

  gold_flights        un vol par ligne : arrivée, départ ou survol, aéroport, point le plus bas
  gold_airport_hourly trafic par aéroport et par heure, avec la météo de cette heure
  gold_anomalies      alertes HOLDING / GO_AROUND du streaming, reliées au vol et à la météo

Un "vol" = les positions successives d'un même avion (icao24) sans trou de plus de 30 minutes.
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, LongType, StringType, StructField, StructType

from .silver import keep_latest

SESSION_GAP_S = 30 * 60      # plus de 30 min sans position = nouveau vol
LOW_LEVEL_AGL_M = 1_500      # sous 1 500 m au-dessus de la piste, près d'un aéroport : arrivée ou départ
CLIMB_DESCENT_M = 300        # écart d'altitude minimum pour dire "descend" ou "monte"
DEMO_ICAO24 = ("fff001", "fff002")   # scripts demo_holding / demo_go_around


# ---------------------------------------------------------------------------
# gold_flights
# ---------------------------------------------------------------------------
def add_flight_session(silver: DataFrame) -> DataFrame:
    """Numérote les vols de chaque avion : nouveau numéro après un trou de plus de 30 min."""
    by_time = Window.partitionBy("icao24").orderBy("snapshot_time")
    running = by_time.rowsBetween(Window.unboundedPreceding, Window.currentRow)
    prev = F.lag("snapshot_time").over(by_time)
    return (silver
            .withColumn("_new_flight", (prev.isNull() | (F.col("snapshot_time") - prev > SESSION_GAP_S)).cast("int"))
            .withColumn("_session", F.sum("_new_flight").over(running))
            .drop("_new_flight"))


def classify_flight(lowest_agl: Column, first_alt: Column, last_alt: Column) -> Column:
    """ARRIVAL / DEPARTURE / LOW_LEVEL / OVERFLIGHT, à partir de ce qu'on a vu près de l'aéroport."""
    return (F.when(lowest_agl.isNull() | (lowest_agl > LOW_LEVEL_AGL_M), "OVERFLIGHT")
             .when(first_alt - last_alt >= CLIMB_DESCENT_M, "ARRIVAL")      # vu haut puis bas
             .when(last_alt - first_alt >= CLIMB_DESCENT_M, "DEPARTURE")    # vu bas puis haut
             .otherwise("LOW_LEVEL"))


def flights_to_gold(silver: DataFrame) -> DataFrame:
    near = F.col("in_terminal_area") & F.col("agl_m").isNotNull()
    # min/max d'une struct = comparaison sur son 1er champ ; when(...) sans otherwise = null, ignoré par min/max
    low_point = F.min(F.when(near, F.struct("agl_m", "nearest_airport", "event_time", "distance_km")))
    first_near = F.min(F.when(near, F.struct("snapshot_time", "altitude_m")))
    last_near = F.max(F.when(near, F.struct("snapshot_time", "altitude_m")))

    flights = (add_flight_session(silver)
               .groupBy("icao24", "_session")
               .agg(F.min("snapshot_time").alias("_start"),
                    F.first("callsign", ignorenulls=True).alias("callsign"),
                    F.first("airline_icao", ignorenulls=True).alias("airline_icao"),
                    F.first("origin_country", ignorenulls=True).alias("origin_country"),
                    F.min("event_time").alias("first_seen"),
                    F.max("event_time").alias("last_seen"),
                    F.count("*").alias("positions"),
                    F.max("altitude_m").alias("max_altitude_m"),
                    F.round(F.avg("velocity_kmh"), 0).alias("avg_velocity_kmh"),
                    F.max(F.col("on_ground").cast("int")).cast("boolean").alias("seen_on_ground"),
                    low_point.alias("_low"), first_near.alias("_first"), last_near.alias("_last")))

    return (flights
            .withColumn("flight_id", F.concat_ws("_", "icao24", F.col("_start").cast("string")))
            .withColumn("airport", F.col("_low.nearest_airport"))
            .withColumn("lowest_agl_m", F.col("_low.agl_m"))
            .withColumn("lowest_at", F.col("_low.event_time"))
            .withColumn("closest_km", F.col("_low.distance_km"))
            .withColumn("flight_type", classify_flight(F.col("_low.agl_m"),
                                                       F.col("_first.altitude_m"), F.col("_last.altitude_m")))
            .withColumn("airport", F.when(F.col("flight_type") != "OVERFLIGHT", F.col("airport")))
            .withColumn("duration_min",
                        F.round((F.unix_timestamp("last_seen") - F.unix_timestamp("first_seen")) / 60, 1))
            .withColumn("flight_date", F.to_date("first_seen"))
            .select("flight_id", "icao24", "callsign", "airline_icao", "origin_country",
                    "flight_type", "airport", "lowest_agl_m", "lowest_at", "closest_km",
                    "first_seen", "last_seen", "duration_min", "positions",
                    "max_altitude_m", "avg_velocity_kmh", "seen_on_ground", "flight_date"))


# ---------------------------------------------------------------------------
# gold_airport_hourly
# ---------------------------------------------------------------------------
def weather_hourly(silver_weather: DataFrame) -> DataFrame:
    """Plusieurs relevés par heure (toutes les 15 min) -> une ligne par aéroport et par heure."""
    return (silver_weather
            .groupBy(F.col("icao").alias("airport"),
                     F.col("observed_date").alias("date"), F.col("observed_hour").alias("hour"))
            .agg(F.round(F.avg("temperature_c"), 1).alias("temperature_c"),
                 F.min("visibility_m").alias("min_visibility_m"),
                 F.max("wind_speed_kt").alias("max_wind_kt"),
                 F.max("wind_gust_kt").alias("max_gust_kt"),
                 F.round(F.sum("precipitation_mm"), 1).alias("precipitation_mm"),
                 F.max("cloud_cover_pct").alias("max_cloud_cover_pct"),
                 F.max(F.col("low_visibility").cast("int")).cast("boolean").alias("low_visibility"),
                 F.max(F.col("strong_wind").cast("int")).cast("boolean").alias("strong_wind")))


def airport_hourly(silver_flights: DataFrame, gold_flights: DataFrame, silver_weather: DataFrame) -> DataFrame:
    near = silver_flights.filter("in_terminal_area")
    traffic = (near
               .groupBy(F.col("nearest_airport").alias("airport"),
                        F.col("event_date").alias("date"), F.col("event_hour").alias("hour"))
               .agg(F.countDistinct("icao24").alias("aircraft"),
                    F.count("*").alias("positions"),
                    F.countDistinct(F.when(F.col("agl_m") < LOW_LEVEL_AGL_M, F.col("icao24")))
                     .alias("low_level_aircraft")))

    movements = (gold_flights
                 .filter(F.col("flight_type").isin("ARRIVAL", "DEPARTURE"))
                 .groupBy("airport", F.to_date("lowest_at").alias("date"), F.hour("lowest_at").alias("hour"))
                 .agg(F.sum((F.col("flight_type") == "ARRIVAL").cast("int")).alias("arrivals"),
                      F.sum((F.col("flight_type") == "DEPARTURE").cast("int")).alias("departures")))

    keys = ["airport", "date", "hour"]
    counts = ["aircraft", "positions", "low_level_aircraft", "arrivals", "departures"]
    out = (traffic.join(movements, keys, "full")
           .join(weather_hourly(silver_weather), keys, "full"))   # garde aussi les heures sans trafic
    for c in counts:
        out = out.withColumn(c, F.coalesce(F.col(c), F.lit(0)).cast("long"))
    return out.select(*keys, *counts, "temperature_c", "min_visibility_m", "max_wind_kt", "max_gust_kt",
                      "precipitation_mm", "max_cloud_cover_pct", "low_visibility", "strong_wind")


# ---------------------------------------------------------------------------
# gold_anomalies
# ---------------------------------------------------------------------------
# Contrat : ingestion/skywatch_ingestion/schemas/flights_alerts.schema.json
ALERT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("alert_type", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("airport", StringType()),
    StructField("detected_at", LongType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("positions", IntegerType()),
    StructField("window_start", LongType()),
    StructField("total_turn_deg", DoubleType()),
    StructField("altitude_m", DoubleType()),
    StructField("lowest_at", LongType()),
    StructField("lowest_altitude_m", DoubleType()),
    StructField("lowest_agl_m", DoubleType()),
    StructField("climb_m", DoubleType()),
])


def anomalies_to_gold(bronze_alerts: DataFrame, gold_flights: DataFrame, silver_weather: DataFrame) -> DataFrame:
    alerts = (bronze_alerts
              .select(F.from_json("raw_json", ALERT_SCHEMA).alias("a"), "kafka_timestamp")
              .select("a.*", "kafka_timestamp")
              .filter(F.col("alert_type").isin("HOLDING", "GO_AROUND") & F.col("detected_at").isNotNull()))
    alerts = keep_latest(alerts, ["alert_type", "icao24", "detected_at"])
    alerts = (alerts
              .withColumn("detected_time", F.timestamp_seconds("detected_at"))
              .withColumn("date", F.to_date("detected_time"))
              .withColumn("hour", F.hour("detected_time"))
              .withColumn("is_demo", F.col("icao24").isin(*DEMO_ICAO24)
                          | F.coalesce(F.col("callsign").startswith("DEMO"), F.lit(False))))

    # Le vol pendant lequel l'alerte a eu lieu (même avion, alerte entre première et dernière position)
    f = gold_flights.select("flight_id", F.col("icao24").alias("_f_icao24"), "first_seen", "last_seen",
                            F.col("callsign").alias("_f_callsign"), "flight_type")
    alerts = alerts.join(f, (alerts.icao24 == f._f_icao24)
                         & alerts.detected_time.between(f.first_seen, f.last_seen), "left")

    weather = weather_hourly(silver_weather).select("airport", "date", "hour", "min_visibility_m",
                                                    "max_wind_kt", "max_gust_kt", "low_visibility", "strong_wind")
    return (alerts.join(weather, ["airport", "date", "hour"], "left")
            .withColumn("callsign", F.coalesce("callsign", "_f_callsign"))
            .select("alert_type", "icao24", "detected_at", "detected_time", "date", "hour",
                    "airport", "callsign", "flight_id", "flight_type", "is_demo",
                    "latitude", "longitude", "positions",
                    "window_start", "total_turn_deg", "altitude_m",
                    "lowest_at", "lowest_altitude_m", "lowest_agl_m", "climb_m",
                    "min_visibility_m", "max_wind_kt", "max_gust_kt", "low_visibility", "strong_wind"))

"""Bronze -> Silver : une ligne propre et typée par position d'avion (et par relevé météo).

Bronze contient les messages Kafka bruts (colonne raw_json), avec :
  - des doublons (même avion, même instantané OpenSky) ;
  - des messages de démo (source = "demo", scripts demo_holding / demo_go_around) ;
  - parfois des positions vides ou hors de la zone.

Silver garde seulement les vraies positions valides, une fois chacune, et ajoute
l'aéroport le plus proche (distance, hauteur au-dessus de la piste).
"""

from __future__ import annotations

from pyspark.sql import Column, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

from .airports import AIRPORTS, LAT_MAX, LAT_MIN, LON_MAX, LON_MIN, TERMINAL_AREA_KM

EARTH_RADIUS_KM = 6371.0
MAX_ALTITUDE_M = 20_000.0  # au-delà : valeur aberrante (les avions de ligne volent sous 13 000 m)
MIN_ALTITUDE_M = -500.0

# Contrat : ingestion/skywatch_ingestion/schemas/flights_raw.schema.json
FLIGHT_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("snapshot_time", LongType()),
    StructField("ingested_at", StringType()),
    StructField("icao24", StringType()),
    StructField("callsign", StringType()),
    StructField("origin_country", StringType()),
    StructField("time_position", LongType()),
    StructField("last_contact", LongType()),
    StructField("longitude", DoubleType()),
    StructField("latitude", DoubleType()),
    StructField("baro_altitude", DoubleType()),
    StructField("on_ground", BooleanType()),
    StructField("velocity", DoubleType()),
    StructField("true_track", DoubleType()),
    StructField("vertical_rate", DoubleType()),
    StructField("geo_altitude", DoubleType()),
    StructField("squawk", StringType()),
    StructField("spi", BooleanType()),
    StructField("position_source", StringType()),
    StructField("category", IntegerType()),
])

# Contrat : ingestion/skywatch_ingestion/schemas/weather_raw.schema.json
WEATHER_SCHEMA = StructType([
    StructField("schema_version", IntegerType()),
    StructField("source", StringType()),
    StructField("icao", StringType()),
    StructField("airport_name", StringType()),
    StructField("city", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("observed_at", LongType()),
    StructField("ingested_at", StringType()),
    StructField("temperature_c", DoubleType()),
    StructField("humidity_pct", DoubleType()),
    StructField("precipitation_mm", DoubleType()),
    StructField("weather_code", IntegerType()),
    StructField("cloud_cover_pct", DoubleType()),
    StructField("pressure_hpa", DoubleType()),
    StructField("visibility_m", DoubleType()),
    StructField("wind_speed_kt", DoubleType()),
    StructField("wind_direction_deg", DoubleType()),
    StructField("wind_gust_kt", DoubleType()),
    StructField("is_day", BooleanType()),
])


# ---------------------------------------------------------------------------
# Outils
# ---------------------------------------------------------------------------
def parse_bronze(bronze: DataFrame, schema: StructType) -> DataFrame:
    """raw_json (texte) -> une colonne par champ. On garde kafka_timestamp pour dédoublonner."""
    return (bronze
            .select(F.from_json("raw_json", schema).alias("m"), "kafka_timestamp")
            .select("m.*", "kafka_timestamp"))


def keep_latest(df: DataFrame, keys: list[str]) -> DataFrame:
    """Une seule ligne par clé : celle arrivée en dernier dans Kafka."""
    w = Window.partitionBy(*keys).orderBy(F.col("kafka_timestamp").desc())
    return df.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")


def haversine_km(lat1: Column, lon1: Column, lat2: float, lon2: float) -> Column:
    """Distance à vol d'oiseau (km) entre une position (colonnes) et un point fixe."""
    dlat = F.radians(F.lit(lat2) - lat1)
    dlon = F.radians(F.lit(lon2) - lon1)
    a = (F.sin(dlat / 2) ** 2
         + F.cos(F.radians(lat1)) * F.cos(F.radians(F.lit(lat2))) * F.sin(dlon / 2) ** 2)
    return 2 * EARTH_RADIUS_KM * F.asin(F.sqrt(a))


# ---------------------------------------------------------------------------
# Vols
# ---------------------------------------------------------------------------
def is_valid_position() -> Column:
    """Position exploitable : identifiant correct, horodatage, coordonnées dans la zone."""
    return (F.col("icao24").rlike("^[0-9a-f]{6}$")
            & F.col("snapshot_time").isNotNull()
            & F.col("latitude").between(LAT_MIN, LAT_MAX)
            & F.col("longitude").between(LON_MIN, LON_MAX))


def clean_flights(parsed: DataFrame) -> DataFrame:
    """Positions brutes -> positions Silver (sans aéroport le plus proche)."""
    flights = parsed.withColumn("icao24", F.lower(F.trim("icao24")))
    flights = flights.filter((F.col("source") == "opensky") & is_valid_position())  # pas de "demo"
    flights = keep_latest(flights, ["icao24", "snapshot_time"])

    callsign = F.trim("callsign")
    altitude = F.coalesce("baro_altitude", "geo_altitude")
    return (flights
            .withColumn("callsign", F.when(callsign != "", callsign))   # OpenSky complète avec des espaces
            .withColumn("airline_icao",                                 # RAM200 -> RAM (compagnie)
                        F.when(F.col("callsign").rlike("^[A-Z]{3}[0-9]"),
                               F.substring("callsign", 1, 3)))
            .withColumn("altitude_m",
                        F.when(altitude.between(MIN_ALTITUDE_M, MAX_ALTITUDE_M), altitude))
            .withColumn("velocity_kmh", F.round(F.col("velocity") * 3.6, 1))
            .withColumn("event_time", F.timestamp_seconds("snapshot_time"))
            .withColumn("event_date", F.to_date("event_time"))
            .withColumn("event_hour", F.hour("event_time"))
            .withColumn("ingested_ts", F.to_timestamp("ingested_at")))


def add_nearest_airport(flights: DataFrame) -> DataFrame:
    """Aéroport le plus proche, distance (km), hauteur au-dessus de sa piste (m), zone d'approche."""
    candidates = F.array(*[
        F.struct(haversine_km(F.col("latitude"), F.col("longitude"), a.lat, a.lon).alias("distance_km"),
                 F.lit(a.icao).alias("icao"),
                 F.lit(float(a.elevation_m)).alias("elevation_m"))
        for a in AIRPORTS
    ])
    nearest = F.array_min(candidates)   # les structs se comparent sur leur 1er champ : la distance
    return (flights
            .withColumn("_nearest", nearest)
            .withColumn("nearest_airport", F.col("_nearest.icao"))
            .withColumn("distance_km", F.round("_nearest.distance_km", 2))
            .withColumn("agl_m", F.round(F.col("altitude_m") - F.col("_nearest.elevation_m"), 0))
            .withColumn("in_terminal_area", F.col("_nearest.distance_km") <= TERMINAL_AREA_KM)
            .drop("_nearest"))


SILVER_FLIGHTS_COLUMNS = [
    "icao24", "snapshot_time", "event_time", "event_date", "event_hour",
    "callsign", "airline_icao", "origin_country",
    "latitude", "longitude", "altitude_m", "baro_altitude", "geo_altitude",
    "on_ground", "velocity", "velocity_kmh", "true_track", "vertical_rate",
    "squawk", "position_source", "category",
    "nearest_airport", "distance_km", "agl_m", "in_terminal_area",
    "ingested_ts", "kafka_timestamp",
]


def flights_to_silver(bronze: DataFrame) -> DataFrame:
    parsed = parse_bronze(bronze, FLIGHT_SCHEMA)
    return add_nearest_airport(clean_flights(parsed)).select(*SILVER_FLIGHTS_COLUMNS)


def flights_quality_report(bronze: DataFrame) -> dict[str, int]:
    """Combien de messages sont écartés, et pourquoi (affiché dans les logs du job)."""
    parsed = parse_bronze(bronze, FLIGHT_SCHEMA).withColumn("icao24", F.lower(F.trim("icao24")))
    row = parsed.agg(
        F.count("*").alias("bronze"),
        F.sum((F.col("source") == "demo").cast("int")).alias("demo"),
        F.sum((F.col("source") == "opensky").cast("int")).alias("opensky"),
        F.sum(((F.col("source") == "opensky") & ~F.coalesce(is_valid_position(), F.lit(False)))
              .cast("int")).alias("invalid_position"),
    ).first()
    return {k: int(row[k] or 0) for k in ["bronze", "demo", "opensky", "invalid_position"]}


# ---------------------------------------------------------------------------
# Météo
# ---------------------------------------------------------------------------
LOW_VISIBILITY_M = 5_000      # sous 5 km : approches plus difficiles
STRONG_WIND_KT = 20
STRONG_GUST_KT = 30

SILVER_WEATHER_COLUMNS = [
    "icao", "observed_at", "observed_time", "observed_date", "observed_hour",
    "temperature_c", "humidity_pct", "precipitation_mm", "weather_code", "cloud_cover_pct",
    "pressure_hpa", "visibility_m", "wind_speed_kt", "wind_direction_deg", "wind_gust_kt", "is_day",
    "low_visibility", "strong_wind", "ingested_ts", "kafka_timestamp",
]


def weather_to_silver(bronze: DataFrame) -> DataFrame:
    weather = parse_bronze(bronze, WEATHER_SCHEMA)
    weather = weather.filter((F.col("source") == "open-meteo")
                             & F.col("icao").rlike("^GM[A-Z]{2}$")
                             & F.col("observed_at").isNotNull())
    weather = keep_latest(weather, ["icao", "observed_at"])
    return (weather
            .withColumn("observed_time", F.timestamp_seconds("observed_at"))
            .withColumn("observed_date", F.to_date("observed_time"))
            .withColumn("observed_hour", F.hour("observed_time"))
            .withColumn("low_visibility", F.col("visibility_m") < LOW_VISIBILITY_M)
            .withColumn("strong_wind", (F.col("wind_speed_kt") >= STRONG_WIND_KT)
                        | (F.col("wind_gust_kt") >= STRONG_GUST_KT))
            .withColumn("ingested_ts", F.to_timestamp("ingested_at"))
            .select(*SILVER_WEATHER_COLUMNS))
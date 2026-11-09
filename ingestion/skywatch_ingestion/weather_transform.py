"""Transforme la réponse Open-Meteo en messages Kafka pour weather.raw.

Comme pour les vols : pas de nettoyage métier ici. On renomme les champs
avec des noms clairs (avec l'unité), on ajoute le code de l'aéroport,
l'heure d'ingestion et la version du schéma.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from . import SCHEMA_VERSION
from .config import Airport
from .transform import utc_now_iso

# nom Open-Meteo → nom dans notre message
FIELD_NAMES: dict[str, str] = {
    "temperature_2m": "temperature_c",
    "relative_humidity_2m": "humidity_pct",
    "precipitation": "precipitation_mm",
    "weather_code": "weather_code",
    "cloud_cover": "cloud_cover_pct",
    "pressure_msl": "pressure_hpa",
    "visibility": "visibility_m",
    "wind_speed_10m": "wind_speed_kt",
    "wind_direction_10m": "wind_direction_deg",
    "wind_gusts_10m": "wind_gust_kt",
    "is_day": "is_day",
}


def location_to_message(airport: Airport, location: dict[str, Any], ingested_at: str) -> dict[str, Any]:
    current = location.get("current") or {}
    msg: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "source": "open-meteo",
        "icao": airport.icao,
        "airport_name": airport.name,
        "city": airport.city,
        "latitude": airport.lat,
        "longitude": airport.lon,
        "observed_at": int(current["time"]),  # heure de la mesure (Unix, s) = event time
        "ingested_at": ingested_at,           # heure de réception = processing time
    }
    for api_name, our_name in FIELD_NAMES.items():
        msg[our_name] = current.get(api_name)
    if msg["is_day"] is not None:
        msg["is_day"] = bool(msg["is_day"])  # Open-Meteo renvoie 0 ou 1
    return msg


def locations_to_messages(airports: Sequence[Airport], locations: Sequence[dict[str, Any]]) -> list[dict[str, Any]]:
    ingested_at = utc_now_iso()
    return [location_to_message(a, loc, ingested_at) for a, loc in zip(airports, locations, strict=True)]

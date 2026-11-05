"""Configuration centrale : zone géographique, aéroports, topics et budget API.

Toutes les valeurs modifiables passent par des variables d'environnement
(fichier .env), jamais par des constantes en dur dans le code des producteurs.
"""

from __future__ import annotations

import math
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class BoundingBox:
    """Zone rectangulaire en degrés (latitude / longitude)."""

    lamin: float
    lomin: float
    lamax: float
    lomax: float

    def __post_init__(self) -> None:
        if not (-90 <= self.lamin < self.lamax <= 90):
            raise ValueError(f"Latitudes invalides : {self.lamin} -> {self.lamax}")
        if not (-180 <= self.lomin < self.lomax <= 180):
            raise ValueError(f"Longitudes invalides : {self.lomin} -> {self.lomax}")

    @property
    def area_sq_deg(self) -> float:
        return (self.lamax - self.lamin) * (self.lomax - self.lomin)

    def contains(self, lat: float | None, lon: float | None) -> bool:
        if lat is None or lon is None:
            return False
        return self.lamin <= lat <= self.lamax and self.lomin <= lon <= self.lomax

    def as_params(self) -> dict[str, float]:
        return {"lamin": self.lamin, "lomin": self.lomin, "lamax": self.lamax, "lomax": self.lomax}


# Zone par défaut : 28°N–36°N × 13°W–1°W
MOROCCO_BBOX = BoundingBox(lamin=28.0, lomin=-13.0, lamax=36.0, lomax=-1.0)


@dataclass(frozen=True)
class Airport:
    icao: str
    name: str
    city: str
    lat: float
    lon: float


AIRPORTS: tuple[Airport, ...] = (
    Airport("GMMN", "Mohammed V", "Casablanca", 33.3675, -7.5900),
    Airport("GMMX", "Menara", "Marrakech", 31.6069, -8.0363),
    Airport("GMME", "Rabat-Salé", "Rabat", 34.0515, -6.7515),
    Airport("GMFF", "Fès-Saïss", "Fès", 33.9273, -4.9779),
    Airport("GMTT", "Ibn Battouta", "Tanger", 35.7269, -5.9169),
    Airport("GMAD", "Al Massira", "Agadir", 30.3250, -9.4131),
)


def opensky_credit_cost(area_sq_deg: float) -> int:
    """Coût en crédits d'un appel /states/all selon la surface de la zone."""
    if area_sq_deg <= 25:
        return 1
    if area_sq_deg <= 100:
        return 2
    if area_sq_deg <= 400:
        return 3
    return 4


def daily_credit_usage(bbox: BoundingBox, interval_s: float) -> int:
    """Crédits consommés en 24 h si on interroge la zone toutes les `interval_s` secondes."""
    calls_per_day = math.ceil(86_400 / interval_s)
    return calls_per_day * opensky_credit_cost(bbox.area_sq_deg)


def min_safe_interval(bbox: BoundingBox, daily_limit: int) -> float:
    """Intervalle minimal (s) qui reste sous la limite quotidienne de crédits."""
    cost = opensky_credit_cost(bbox.area_sq_deg)
    max_calls = daily_limit // cost
    if max_calls <= 0:
        raise ValueError("Limite de crédits trop basse pour un seul appel")
    return 86_400 / max_calls


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    return float(raw) if raw not in (None, "") else default


@dataclass(frozen=True)
class Settings:
    kafka_bootstrap: str = "localhost:9092"
    topic_flights: str = "flights.raw"
    topic_weather: str = "weather.raw"

    opensky_client_id: str | None = None
    opensky_client_secret: str | None = None
    opensky_api_url: str = "https://opensky-network.org/api"
    opensky_token_url: str = (
        "https://auth.opensky-network.org/auth/realms/opensky-network/"
        "protocol/openid-connect/token"
    )
    flights_interval_s: float = 45.0
    weather_interval_s: float = 3600.0
    bbox: BoundingBox = field(default_factory=lambda: MOROCCO_BBOX)

    open_meteo_url: str = "https://api.open-meteo.com/v1/forecast"

    @property
    def authenticated(self) -> bool:
        return bool(self.opensky_client_id and self.opensky_client_secret)

    @property
    def daily_credit_limit(self) -> int:
        # 4 000 crédits/jour avec un client OAuth2, 400 en anonyme.
        return 4000 if self.authenticated else 400

    @classmethod
    def from_env(cls) -> Settings:
        bbox = BoundingBox(
            lamin=_env_float("BBOX_LAMIN", MOROCCO_BBOX.lamin),
            lomin=_env_float("BBOX_LOMIN", MOROCCO_BBOX.lomin),
            lamax=_env_float("BBOX_LAMAX", MOROCCO_BBOX.lamax),
            lomax=_env_float("BBOX_LOMAX", MOROCCO_BBOX.lomax),
        )
        return cls(
            kafka_bootstrap=os.getenv("KAFKA_BOOTSTRAP_SERVERS", cls.kafka_bootstrap),
            topic_flights=os.getenv("TOPIC_FLIGHTS_RAW", cls.topic_flights),
            topic_weather=os.getenv("TOPIC_WEATHER_RAW", cls.topic_weather),
            opensky_client_id=os.getenv("OPENSKY_CLIENT_ID") or None,
            opensky_client_secret=os.getenv("OPENSKY_CLIENT_SECRET") or None,
            flights_interval_s=_env_float("FLIGHTS_POLL_INTERVAL_S", cls.flights_interval_s),
            weather_interval_s=_env_float("WEATHER_POLL_INTERVAL_S", cls.weather_interval_s),
            bbox=bbox,
        )
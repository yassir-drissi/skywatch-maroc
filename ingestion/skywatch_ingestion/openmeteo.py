"""Client Open-Meteo : météo actuelle des aéroports, en un seul appel.

Open-Meteo est gratuit et sans clé. On demande toutes les coordonnées
d'un coup (listes de latitudes et de longitudes) : la réponse est alors
une liste, avec un élément par aéroport, dans le même ordre.
"""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

import requests

from .config import Airport

# Variables demandées (noms Open-Meteo). Vent en nœuds, comme en aviation.
CURRENT_VARIABLES: tuple[str, ...] = (
    "temperature_2m",
    "relative_humidity_2m",
    "precipitation",
    "weather_code",
    "cloud_cover",
    "pressure_msl",
    "visibility",
    "wind_speed_10m",
    "wind_direction_10m",
    "wind_gusts_10m",
    "is_day",
)


class OpenMeteoClient:
    def __init__(self, api_url: str, session: requests.Session | None = None):
        self._api_url = api_url
        self._session = session or requests.Session()

    def get_current(self, airports: Sequence[Airport]) -> list[dict[str, Any]]:
        """Renvoie la météo actuelle : un dictionnaire par aéroport, dans l'ordre de `airports`."""
        resp = self._session.get(
            self._api_url,
            params={
                "latitude": ",".join(str(a.lat) for a in airports),
                "longitude": ",".join(str(a.lon) for a in airports),
                "current": ",".join(CURRENT_VARIABLES),
                "wind_speed_unit": "kn",
                "timeformat": "unixtime",  # heures en secondes Unix, comme snapshot_time d'OpenSky
                "timezone": "GMT",
            },
            timeout=20,
        )
        resp.raise_for_status()
        payload = resp.json()
        # Avec un seul lieu, Open-Meteo renvoie un objet au lieu d'une liste.
        locations = payload if isinstance(payload, list) else [payload]
        if len(locations) != len(airports):
            raise ValueError(f"Open-Meteo a renvoyé {len(locations)} lieux pour {len(airports)} aéroports")
        return locations

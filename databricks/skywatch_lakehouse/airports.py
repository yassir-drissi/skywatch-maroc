"""Aéroports surveillés et zone géographique.

Mêmes valeurs que ingestion/skywatch_ingestion/config.py (Python) et
streaming/.../Airports.scala (Scala). Un test vérifie qu'elles restent identiques.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Airport:
    icao: str
    name: str
    city: str
    lat: float
    lon: float
    elevation_m: float  # altitude de la piste : OpenSky donne l'altitude au-dessus de la mer


AIRPORTS: tuple[Airport, ...] = (
    Airport("GMMN", "Mohammed V", "Casablanca", 33.3675, -7.5900, 200),
    Airport("GMMX", "Menara", "Marrakech", 31.6069, -8.0363, 466),
    Airport("GMME", "Rabat-Salé", "Rabat", 34.0515, -6.7515, 84),
    Airport("GMFF", "Fès-Saïss", "Fès", 33.9273, -4.9779, 579),
    Airport("GMTT", "Ibn Battouta", "Tanger", 35.7269, -5.9169, 21),
    Airport("GMAD", "Al Massira", "Agadir", 30.3250, -9.4131, 23),
)

# Zone interrogée par le producteur : 28°N–36°N × 13°W–1°W
LAT_MIN, LAT_MAX = 28.0, 36.0
LON_MIN, LON_MAX = -13.0, -1.0

# Rayon autour d'un aéroport pour dire qu'un avion est "dans sa zone" (même valeur que Scala)
TERMINAL_AREA_KM = 60.0

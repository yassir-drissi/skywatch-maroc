"""Envoie une FAUSSE attente en circuit près de Tanger dans flights.raw.

Sert à démontrer l'alerte HOLDING de Spark sans attendre un vrai avion en attente.
Les messages ont source="demo" : la couche Silver devra les retirer.

    python -m skywatch_ingestion.demo_holding             # envoie dans Kafka (KAFKA_BOOTSTRAP_SERVERS)
    python -m skywatch_ingestion.demo_holding --dry-run   # affiche sans Kafka
"""

from __future__ import annotations

import argparse
import math
import time
from typing import Any

from dotenv import load_dotenv

from . import SCHEMA_VERSION
from .config import Settings
from .kafka_sink import KafkaSink, Sink, StdoutSink, publish
from .transform import utc_now_iso

DEMO_ICAO24 = "fff001"
DEMO_CALLSIGN = "DEMO01"
CENTER_LAT, CENTER_LON = 35.80, -5.80  # environ 13 km de l'aéroport de Tanger (GMTT)
RADIUS_DEG = 0.03                       # environ 3 km


def holding_messages(now: int, positions: int = 12, step_s: int = 45, turn_per_step: float = 90.0,
                     altitude_m: float = 2500.0) -> list[dict[str, Any]]:
    """Un avion qui tourne en rond : +90° de cap toutes les 45 s, à 2 500 m.

    Les heures sont dans le passé proche (la dernière = maintenant), pour que le
    watermark de Spark accepte les messages.
    """
    ingested_at = utc_now_iso()
    messages = []
    for i in range(positions):
        t = now - (positions - 1 - i) * step_s
        track = (i * turn_per_step) % 360
        angle = math.radians(track)
        messages.append({
            "schema_version": SCHEMA_VERSION,
            "source": "demo",
            "snapshot_time": t,
            "ingested_at": ingested_at,
            "icao24": DEMO_ICAO24,
            "callsign": DEMO_CALLSIGN,
            "origin_country": "Morocco",
            "time_position": t,
            "last_contact": t,
            "longitude": round(CENTER_LON + RADIUS_DEG * math.sin(angle), 5),
            "latitude": round(CENTER_LAT + RADIUS_DEG * math.cos(angle), 5),
            "baro_altitude": altitude_m,
            "on_ground": False,
            "velocity": 90.0,
            "true_track": track,
            "vertical_rate": 0.0,
            "geo_altitude": altitude_m + 50,
            "squawk": None,
            "spi": False,
            "position_source": "ADS-B",
            "category": 0,
        })
    return messages


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="affiche les messages au lieu d'écrire dans Kafka")
    args = parser.parse_args(argv)

    load_dotenv()
    settings = Settings.from_env()
    messages = holding_messages(int(time.time()))
    sink: Sink = StdoutSink(limit=len(messages)) if args.dry_run else KafkaSink(settings.kafka_bootstrap,
                                                                                "skywatch-demo")
    sent = publish(sink, settings.topic_flights, "icao24", messages)
    print(f"{sent} positions de démo envoyées dans {settings.topic_flights} (avion {DEMO_ICAO24})")


if __name__ == "__main__":
    main()

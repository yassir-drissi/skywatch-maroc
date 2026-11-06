"""Envoie une FAUSSE remise de gaz à Tanger dans flights.raw.

L'avion descend vers la piste 28 de Tanger jusqu'à 300 m, puis remonte sans atterrir.
Sert à démontrer l'alerte GO_AROUND de Spark. Les messages ont source="demo".

    python -m skywatch_ingestion.demo_go_around             # envoie dans Kafka (KAFKA_BOOTSTRAP_SERVERS)
    python -m skywatch_ingestion.demo_go_around --dry-run   # affiche sans Kafka
"""

from __future__ import annotations

import argparse
import time
from typing import Any

from dotenv import load_dotenv

from . import SCHEMA_VERSION
from .config import Settings
from .kafka_sink import KafkaSink, Sink, StdoutSink, publish
from .transform import utc_now_iso

DEMO_ICAO24 = "fff002"
DEMO_CALLSIGN = "DEMO02"
ALTITUDES_M = (1500.0, 1200.0, 900.0, 600.0, 300.0, 600.0, 900.0, 1200.0)  # descente, point bas, remontée
START_LAT, START_LON = 35.73, -5.80   # 10 km à l'est de la piste
LON_STEP = -0.02                      # l'avion avance vers l'ouest (piste 28)


def go_around_messages(now: int, step_s: int = 45) -> list[dict[str, Any]]:
    """Une position toutes les 45 s, la dernière à l'instant `now`."""
    ingested_at = utc_now_iso()
    n = len(ALTITUDES_M)
    messages = []
    for i, altitude in enumerate(ALTITUDES_M):
        t = now - (n - 1 - i) * step_s
        climbing = i > 0 and altitude > ALTITUDES_M[i - 1]
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
            "longitude": round(START_LON + i * LON_STEP, 5),
            "latitude": START_LAT,
            "baro_altitude": altitude,
            "on_ground": False,
            "velocity": 75.0,
            "true_track": 280.0,
            "vertical_rate": 6.5 if climbing else -6.5,
            "geo_altitude": altitude + 50,
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
    messages = go_around_messages(int(time.time()))
    sink: Sink = StdoutSink(limit=len(messages)) if args.dry_run else KafkaSink(settings.kafka_bootstrap,
                                                                                "skywatch-demo")
    sent = publish(sink, settings.topic_flights, "icao24", messages)
    print(f"{sent} positions de démo envoyées dans {settings.topic_flights} (avion {DEMO_ICAO24})")


if __name__ == "__main__":
    main()
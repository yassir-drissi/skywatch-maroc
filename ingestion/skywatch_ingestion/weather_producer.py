"""Producteur Open-Meteo → topic Kafka weather.raw (un message par aéroport).

    python -m skywatch_ingestion.weather_producer            # boucle continue (1 fois par heure)
    python -m skywatch_ingestion.weather_producer --once     # un seul appel
    python -m skywatch_ingestion.weather_producer --dry-run  # affiche sans Kafka
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Sequence

from dotenv import load_dotenv

from .config import AIRPORTS, Airport, Settings
from .kafka_sink import KafkaSink, Sink, StdoutSink, publish
from .openmeteo import OpenMeteoClient
from .runner import StopFlag, run_loop
from .weather_transform import locations_to_messages

log = logging.getLogger("skywatch.weather")


class WeatherTick:
    """Un cycle : appeler Open-Meteo, transformer, envoyer dans Kafka."""

    def __init__(self, client: OpenMeteoClient, sink: Sink, settings: Settings,
                 airports: Sequence[Airport] = AIRPORTS):
        self.client = client
        self.sink = sink
        self.settings = settings
        self.airports = airports
        self.total_messages = 0

    def __call__(self) -> float | None:
        locations = self.client.get_current(self.airports)
        messages = locations_to_messages(self.airports, locations)
        sent = publish(self.sink, self.settings.topic_weather, "icao", messages)
        self.total_messages += sent
        resume = " ".join(f"{m['icao']}={m['wind_speed_kt']}kt" for m in messages)
        log.info("météo envoyée : %s aéroports (%s) total_envoyés=%s", sent, resume, self.total_messages)
        return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="un seul appel puis arrêt")
    parser.add_argument("--dry-run", action="store_true", help="affiche les messages au lieu d'écrire dans Kafka")
    args = parser.parse_args(argv)

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    log.info("Météo de %s aéroports toutes les %.0f s", len(AIRPORTS), settings.weather_interval_s)

    sink: Sink = StdoutSink(limit=len(AIRPORTS)) if args.dry_run else KafkaSink(settings.kafka_bootstrap,
                                                                                "skywatch-weather")
    stop = StopFlag()
    stop.install_signal_handlers()
    client = OpenMeteoClient(settings.open_meteo_url)
    run_loop(WeatherTick(client, sink, settings), settings.weather_interval_s, stop, once=args.once)
    sink.flush()


if __name__ == "__main__":
    main()

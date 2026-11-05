"""Producteur OpenSky → topic Kafka flights.raw.

    python -m skywatch_ingestion.flights_producer            # boucle continue
    python -m skywatch_ingestion.flights_producer --once     # un seul appel
    python -m skywatch_ingestion.flights_producer --dry-run  # affiche sans Kafka
"""

from __future__ import annotations

import argparse
import logging

from dotenv import load_dotenv

from .config import Settings, daily_credit_usage, min_safe_interval
from .kafka_sink import KafkaSink, Sink, StdoutSink, publish
from .opensky import OpenSkyClient, RateLimited, build_client
from .runner import StopFlag, run_loop
from .transform import states_to_messages

log = logging.getLogger("skywatch.flights")

LOW_CREDITS_WARNING = 100  # sous ce seuil, on prévient dans les logs


def effective_interval(settings: Settings) -> float:
    """Intervalle réel : celui demandé, relevé si besoin pour tenir dans le budget du jour."""
    floor = min_safe_interval(settings.bbox, settings.daily_credit_limit)
    if settings.flights_interval_s < floor:
        log.warning("Intervalle %.0f s trop court pour %s crédits/jour : relevé à %.0f s",
                    settings.flights_interval_s, settings.daily_credit_limit, floor)
        return floor
    return settings.flights_interval_s


class FlightsTick:
    """Un cycle : appeler OpenSky, transformer, envoyer dans Kafka."""

    def __init__(self, client: OpenSkyClient, sink: Sink, settings: Settings):
        self.client = client
        self.sink = sink
        self.settings = settings
        self.total_messages = 0

    def __call__(self) -> float | None:
        try:
            resp = self.client.get_states(self.settings.bbox)
        except RateLimited as exc:
            log.warning("%s", exc)
            return exc.retry_after_s  # le runner attendra ce délai

        messages = states_to_messages(resp.states, resp.time)
        sent = publish(self.sink, self.settings.topic_flights, "icao24", messages)
        self.total_messages += sent
        en_vol = sum(1 for m in messages if not m["on_ground"])
        log.info("snapshot=%s avions=%s en_vol=%s crédits_restants=%s total_envoyés=%s",
                 resp.time, sent, en_vol, resp.remaining_credits, self.total_messages)
        if resp.remaining_credits is not None and resp.remaining_credits < LOW_CREDITS_WARNING:
            log.warning("Plus que %s crédits OpenSky aujourd'hui", resp.remaining_credits)
        return None


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--once", action="store_true", help="un seul appel puis arrêt")
    parser.add_argument("--dry-run", action="store_true", help="affiche les messages au lieu d'écrire dans Kafka")
    args = parser.parse_args(argv)

    load_dotenv()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    settings = Settings.from_env()
    interval = effective_interval(settings)
    log.info("Intervalle %.0f s, budget %s/%s crédits par jour, auth=%s", interval,
             daily_credit_usage(settings.bbox, interval), settings.daily_credit_limit,
             "OAuth2" if settings.authenticated else "anonyme")

    sink: Sink = StdoutSink() if args.dry_run else KafkaSink(settings.kafka_bootstrap, "skywatch-flights")
    stop = StopFlag()
    stop.install_signal_handlers()
    run_loop(FlightsTick(build_client(settings), sink, settings), interval, stop, once=args.once)
    sink.flush()


if __name__ == "__main__":
    main()

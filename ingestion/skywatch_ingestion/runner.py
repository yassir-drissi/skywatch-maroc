"""Boucle de polling commune : intervalle fixe, arrêt propre, backoff sur erreur."""

from __future__ import annotations

import logging
import signal
import threading
import time
from collections.abc import Callable

log = logging.getLogger(__name__)


class StopFlag:
    def __init__(self) -> None:
        self._event = threading.Event()

    def install_signal_handlers(self) -> None:
        def handler(signum, _frame):
            log.info("Signal %s reçu, arrêt après le cycle en cours", signum)
            self._event.set()

        signal.signal(signal.SIGINT, handler)
        signal.signal(signal.SIGTERM, handler)

    def set(self) -> None:
        self._event.set()

    @property
    def is_set(self) -> bool:
        return self._event.is_set()

    def wait(self, seconds: float) -> bool:
        """Attend `seconds` ou jusqu'à l'arrêt. Renvoie True si l'arrêt est demandé."""
        return self._event.wait(max(0.0, seconds))


def run_loop(
    tick: Callable[[], float | None],
    interval_s: float,
    stop: StopFlag,
    once: bool = False,
    max_backoff_s: float = 300.0,
    clock: Callable[[], float] = time.monotonic,
) -> None:
    """Appelle `tick` toutes les `interval_s` secondes.

    `tick` peut renvoyer un délai (en secondes) pour imposer une attente plus
    longue, par exemple après un 429. Les exceptions déclenchent un backoff
    exponentiel plafonné, sans arrêter le producteur.
    """
    failures = 0
    while not stop.is_set:
        started = clock()
        try:
            override = tick()
            failures = 0
        except Exception:  # noqa: BLE001 — un producteur 24/7 ne doit pas mourir sur une erreur réseau
            failures += 1
            override = min(max_backoff_s, interval_s * (2 ** (failures - 1)))
            log.exception("Cycle en échec (%s d'affilée), nouvel essai dans %.0f s", failures, override)
        if once:
            return
        delay = override if override is not None else interval_s - (clock() - started)
        if stop.wait(delay):
            return

"""Transforme les réponses brutes d'OpenSky en messages Kafka nommés.

On garde la donnée telle que l'API la donne : pas de nettoyage métier ici,
c'est le rôle de Spark et de la couche Silver. On nomme les champs, on ajoute
l'heure d'ingestion et la version du schéma.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import UTC, datetime
from typing import Any

from . import SCHEMA_VERSION
from .opensky import STATE_FIELDS

POSITION_SOURCES = {0: "ADS-B", 1: "ASTERIX", 2: "MLAT", 3: "FLARM"}


def utc_now_iso() -> str:
    return datetime.now(UTC).isoformat(timespec="milliseconds")


def state_to_message(state: list[Any], snapshot_time: int, ingested_at: str) -> dict[str, Any] | None:
    """Convertit un « state vector » OpenSky (liste) en dictionnaire nommé.

    Renvoie None si l'entrée est inutilisable (pas d'identifiant avion).
    """
    if not state or not state[0]:
        return None

    # Sans extended=1, la liste n'a que 17 cases : on complète avec None.
    padded = list(state) + [None] * (len(STATE_FIELDS) - len(state))
    rec = dict(zip(STATE_FIELDS, padded, strict=False))

    callsign = rec["callsign"]
    rec["callsign"] = (callsign.strip() or None) if isinstance(callsign, str) else None
    rec["icao24"] = str(rec["icao24"]).lower()
    rec["position_source"] = POSITION_SOURCES.get(rec["position_source"], rec["position_source"])
    rec.pop("sensors", None)  # toujours null sur /states/all

    return {
        "schema_version": SCHEMA_VERSION,
        "source": "opensky",
        "snapshot_time": snapshot_time,
        "ingested_at": ingested_at,
        **rec,
    }


def states_to_messages(states: Iterable[list[Any]], snapshot_time: int) -> list[dict[str, Any]]:
    ingested_at = utc_now_iso()
    messages = []
    for state in states:
        msg = state_to_message(state, snapshot_time, ingested_at)
        if msg is not None:
            messages.append(msg)
    return messages

"""Tests de la trajectoire de démo : elle doit vraiment ressembler à une attente en circuit."""

import math
from itertools import pairwise

from skywatch_ingestion.config import AIRPORTS
from skywatch_ingestion.demo_holding import DEMO_ICAO24, holding_messages

NOW = 1794000000


def heading_delta(a: float, b: float) -> float:
    """Même calcul que Geo.headingDelta côté Scala."""
    return ((b - a) % 360 + 540) % 360 - 180


def distance_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def test_positions_every_45_seconds_ending_now():
    msgs = holding_messages(NOW)
    times = [m["snapshot_time"] for m in msgs]
    assert len(msgs) == 12
    assert times[-1] == NOW
    assert all(b - a == 45 for a, b in pairwise(times))


def test_total_turn_is_more_than_one_full_circle():
    msgs = holding_messages(NOW)
    tracks = [m["true_track"] for m in msgs]
    total = sum(heading_delta(a, b) for a, b in pairwise(tracks))
    assert total >= 360        # 11 virages de 90° = 990°


def test_demo_plane_is_low_and_near_tangier():
    tangier = next(a for a in AIRPORTS if a.icao == "GMTT")
    for m in holding_messages(NOW):
        assert m["baro_altitude"] < 6000
        assert m["on_ground"] is False
        assert distance_km(m["latitude"], m["longitude"], tangier.lat, tangier.lon) < 60


def test_demo_messages_are_marked_as_demo():
    msgs = holding_messages(NOW)
    assert {m["source"] for m in msgs} == {"demo"}      # Silver pourra les filtrer
    assert {m["icao24"] for m in msgs} == {DEMO_ICAO24}

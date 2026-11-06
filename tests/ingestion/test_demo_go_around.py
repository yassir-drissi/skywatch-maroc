"""Tests de la trajectoire de démo : elle doit vraiment ressembler à une remise de gaz."""

import math
from itertools import pairwise

from skywatch_ingestion.config import AIRPORTS
from skywatch_ingestion.demo_go_around import DEMO_ICAO24, go_around_messages

NOW = 1794000000
TANGIER_ELEVATION_M = 21


def distance_km(lat1, lon1, lat2, lon2):
    r = 6371.0
    dlat, dlon = math.radians(lat2 - lat1), math.radians(lon2 - lon1)
    a = math.sin(dlat / 2) ** 2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon / 2) ** 2
    return 2 * r * math.asin(math.sqrt(a))


def test_positions_every_45_seconds_ending_now():
    times = [m["snapshot_time"] for m in go_around_messages(NOW)]
    assert times[-1] == NOW
    assert all(b - a == 45 for a, b in pairwise(times))


def test_profile_descends_then_climbs_without_landing():
    msgs = go_around_messages(NOW)
    alts = [m["baro_altitude"] for m in msgs]
    low = alts.index(min(alts))
    assert max(alts[:low]) - alts[low] >= 150      # descente d'au moins 150 m
    assert max(alts[low + 1:]) - alts[low] >= 300  # remontée d'au moins 300 m
    assert not any(m["on_ground"] for m in msgs)   # jamais au sol


def test_lowest_point_is_close_to_tangier_runway():
    tangier = next(a for a in AIRPORTS if a.icao == "GMTT")
    msgs = go_around_messages(NOW)
    lowest = min(msgs, key=lambda m: m["baro_altitude"])
    assert lowest["baro_altitude"] - TANGIER_ELEVATION_M <= 600
    assert distance_km(lowest["latitude"], lowest["longitude"], tangier.lat, tangier.lon) < 15


def test_demo_messages_are_marked_as_demo():
    msgs = go_around_messages(NOW)
    assert {m["source"] for m in msgs} == {"demo"}
    assert {m["icao24"] for m in msgs} == {DEMO_ICAO24}

import json
from pathlib import Path

import jsonschema
import pytest

from skywatch_ingestion.transform import states_to_messages

SCHEMA_PATH = Path(__file__).parents[2] / "ingestion" / "skywatch_ingestion" / "schemas" / "flights_raw.schema.json"

SNAPSHOT = 1762336800
STATES = [
    # vol RAM en approche de Casablanca (18 cases, avec extended=1)
    ["020118", "RAM200  ", "Morocco", 1762336798, 1762336799, -7.4812, 33.4521, 1524.0, False,
     92.6, 186.3, -4.55, None, 1600.2, "2341", False, 0, 4],
    # Ryanair au sol
    ["4CA7B4", "RYR8NP  ", "Ireland", 1762336790, 1762336795, -7.5901, 33.3672, None, True,
     0.0, 92.8, None, None, None, None, False, 0, 3],
    # entrée inutilisable : pas d'icao24
    ["", None, None, None, 0, None, None, None, False, None, None, None, None, None, None, False, 0],
]


@pytest.fixture
def flights_schema():
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_states_are_named_and_cleaned():
    msgs = states_to_messages(STATES, SNAPSHOT)

    assert len(msgs) == 2                     # l'entrée sans icao24 est ignorée
    ram = msgs[0]
    assert ram["callsign"] == "RAM200"        # espaces de fin retirés
    assert ram["latitude"] == 33.4521 and ram["longitude"] == -7.4812
    assert ram["position_source"] == "ADS-B"  # 0 → libellé lisible
    assert ram["snapshot_time"] == SNAPSHOT
    assert "sensors" not in ram
    assert msgs[1]["icao24"] == "4ca7b4"      # toujours en minuscules
    assert msgs[1]["on_ground"] is True


def test_short_state_without_category_is_padded():
    state = ["abc123", None, "France", 1, 2, 1.0, 30.0, 100.0, False, 50.0, 10.0, 0.0, None, 110.0, None, False, 2]
    (msg,) = states_to_messages([state], 3)
    assert msg["category"] is None            # 17 cases → complété avec None
    assert msg["callsign"] is None
    assert msg["position_source"] == "MLAT"


def test_messages_match_json_schema(flights_schema):
    for msg in states_to_messages(STATES, SNAPSHOT):
        jsonschema.validate(msg, flights_schema, format_checker=jsonschema.FormatChecker())


def test_schema_rejects_bad_message(flights_schema):
    (msg,) = states_to_messages(STATES[:1], SNAPSHOT)
    msg["latitude"] = 123.0                   # latitude impossible
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(msg, flights_schema)

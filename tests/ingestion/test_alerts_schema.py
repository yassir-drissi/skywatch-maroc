"""Le contrat du topic flights.alerts, vérifié sur des messages identiques à ceux de Spark."""

import json
from pathlib import Path

import jsonschema
import pytest

SCHEMA_PATH = Path(__file__).parents[2] / "ingestion" / "skywatch_ingestion" / "schemas" / "flights_alerts.schema.json"

# Même format que les messages produits par HoldingJob et GoAroundJob avec les scripts de démo
HOLDING = {
    "schema_version": 1, "alert_type": "HOLDING", "icao24": "fff001", "callsign": "DEMO01", "airport": "GMTT",
    "detected_at": 1794002048, "window_start": 1794001553, "total_turn_deg": 990.0, "altitude_m": 2500.0,
    "latitude": 35.8, "longitude": -5.83, "positions": 12,
}
GO_AROUND = {
    "schema_version": 1, "alert_type": "GO_AROUND", "icao24": "fff002", "callsign": "DEMO02", "airport": "GMTT",
    "detected_at": 1794003600, "lowest_at": 1794003465, "lowest_altitude_m": 300.0, "lowest_agl_m": 279.0,
    "climb_m": 900.0, "latitude": 35.73, "longitude": -5.88, "positions": 8,
}


@pytest.fixture
def schema():
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


def test_holding_and_go_around_alerts_are_valid(schema):
    jsonschema.validate(HOLDING, schema)
    jsonschema.validate(GO_AROUND, schema)


def test_null_fields_may_be_absent(schema):
    # Spark n'écrit pas les champs null : un avion sans indicatif n'a pas de « callsign »
    msg = {k: v for k, v in HOLDING.items() if k not in ("callsign", "altitude_m")}
    jsonschema.validate(msg, schema)


def test_type_specific_fields_are_required(schema):
    incomplete = {k: v for k, v in GO_AROUND.items() if k != "climb_m"}
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(incomplete, schema)


def test_unknown_alert_type_is_rejected(schema):
    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate({**HOLDING, "alert_type": "CRASH"}, schema)

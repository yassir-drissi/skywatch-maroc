"""Tests du producteur météo, sans réseau ni Kafka."""

import json
from pathlib import Path

import jsonschema
import pytest

from skywatch_ingestion import weather_producer
from skywatch_ingestion.config import AIRPORTS, Settings
from skywatch_ingestion.openmeteo import OpenMeteoClient
from skywatch_ingestion.weather_producer import WeatherTick
from skywatch_ingestion.weather_transform import locations_to_messages

SCHEMA_PATH = Path(__file__).parents[2] / "ingestion" / "skywatch_ingestion" / "schemas" / "weather_raw.schema.json"
API = "https://api.open-meteo.com/v1/forecast"
OBSERVED = 1794000600


def fake_location(wind_kt=12.4, visibility=24140.0, is_day=1):
    """Un élément de la réponse Open-Meteo (format réel, timeformat=unixtime)."""
    return {
        "latitude": 33.375, "longitude": -7.625, "timezone": "GMT", "utc_offset_seconds": 0,
        "current_units": {"time": "unixtime", "wind_speed_10m": "kn"},
        "current": {
            "time": OBSERVED, "interval": 900,
            "temperature_2m": 21.3, "relative_humidity_2m": 64, "precipitation": 0.0,
            "weather_code": 1, "cloud_cover": 20, "pressure_msl": 1018.2, "visibility": visibility,
            "wind_speed_10m": wind_kt, "wind_direction_10m": 250, "wind_gusts_10m": 21.0, "is_day": is_day,
        },
    }


# ---------- faux objets ----------

class FakeResponse:
    def __init__(self, payload, status=200):
        self._payload = payload
        self.status_code = status

    def json(self):
        return self._payload

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"HTTP {self.status_code}")


class FakeSession:
    """Remplace requests.Session : renvoie une réponse fixe et garde les paramètres envoyés."""

    def __init__(self, response):
        self.response = response
        self.params = None

    def get(self, url, params=None, timeout=None):
        self.params = params
        return self.response


class FakeSink:
    def __init__(self):
        self.sent = []

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


@pytest.fixture
def weather_schema():
    return json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))


# ---------- client Open-Meteo ----------

def test_client_asks_all_airports_in_one_call_with_knots():
    session = FakeSession(FakeResponse([fake_location() for _ in AIRPORTS]))

    locations = OpenMeteoClient(API, session=session).get_current(AIRPORTS)

    assert len(locations) == 6
    assert session.params["latitude"].count(",") == 5      # 6 latitudes dans un seul appel
    assert session.params["wind_speed_unit"] == "kn"
    assert session.params["timeformat"] == "unixtime"


def test_client_accepts_single_object_for_one_airport():
    session = FakeSession(FakeResponse(fake_location()))   # 1 lieu → objet, pas liste

    assert len(OpenMeteoClient(API, session=session).get_current(AIRPORTS[:1])) == 1


def test_client_rejects_wrong_number_of_locations():
    session = FakeSession(FakeResponse([fake_location()]))

    with pytest.raises(ValueError):
        OpenMeteoClient(API, session=session).get_current(AIRPORTS)


def test_client_raises_on_http_error():
    session = FakeSession(FakeResponse({"error": True}, status=503))

    with pytest.raises(RuntimeError):
        OpenMeteoClient(API, session=session).get_current(AIRPORTS)


# ---------- transformation ----------

def test_messages_are_named_with_units(weather_schema):
    msgs = locations_to_messages(AIRPORTS, [fake_location() for _ in AIRPORTS])

    assert [m["icao"] for m in msgs] == ["GMMN", "GMMX", "GMME", "GMFF", "GMTT", "GMAD"]
    casa = msgs[0]
    assert casa["city"] == "Casablanca"
    assert casa["observed_at"] == OBSERVED
    assert casa["wind_speed_kt"] == 12.4 and casa["wind_gust_kt"] == 21.0
    assert casa["visibility_m"] == 24140.0
    assert casa["is_day"] is True                           # 1 → True
    for m in msgs:
        jsonschema.validate(m, weather_schema)              # respecte le contrat weather.raw


def test_missing_values_stay_null(weather_schema):
    loc = fake_location()
    del loc["current"]["visibility"]                        # variable absente de la réponse

    msg = locations_to_messages(AIRPORTS[:1], [loc])[0]

    assert msg["visibility_m"] is None                      # on ne devine pas : null
    jsonschema.validate(msg, weather_schema)


def test_schema_rejects_bad_message(weather_schema):
    msg = locations_to_messages(AIRPORTS[:1], [fake_location()])[0]
    msg["icao"] = "LFPG"                                     # aéroport hors Maroc

    with pytest.raises(jsonschema.ValidationError):
        jsonschema.validate(msg, weather_schema)


# ---------- producteur ----------

def test_tick_publishes_one_message_per_airport_with_icao_key():
    session = FakeSession(FakeResponse([fake_location() for _ in AIRPORTS]))
    sink = FakeSink()
    tick = WeatherTick(OpenMeteoClient(API, session=session), sink, Settings())

    assert tick() is None
    assert {topic for topic, _, _ in sink.sent} == {"weather.raw"}
    assert [key for _, key, _ in sink.sent] == ["GMMN", "GMMX", "GMME", "GMFF", "GMTT", "GMAD"]
    assert tick.total_messages == 6


def test_main_once_dry_run_makes_one_call(monkeypatch):
    calls = []

    class FakeClient:
        def __init__(self, api_url):
            pass

        def get_current(self, airports):
            calls.append(1)
            return [fake_location() for _ in airports]

    monkeypatch.setattr(weather_producer, "OpenMeteoClient", FakeClient)
    monkeypatch.setattr(weather_producer, "load_dotenv", lambda: None)

    weather_producer.main(["--once", "--dry-run"])

    assert len(calls) == 1

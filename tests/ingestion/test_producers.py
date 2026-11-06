"""Tests du producteur des vols et de la boucle runner, sans réseau ni Kafka.

On remplace OpenSky, Kafka et l'horloge par des « faux » objets :
les tests sont rapides et ne consomment aucun crédit OpenSky.
"""

from skywatch_ingestion import flights_producer
from skywatch_ingestion.config import Settings
from skywatch_ingestion.flights_producer import FlightsTick, effective_interval
from skywatch_ingestion.opensky import RateLimited, StatesResponse
from skywatch_ingestion.runner import StopFlag, run_loop

SNAPSHOT = 1762336800
STATES = [
    ["020118", "RAM200  ", "Morocco", 1762336798, 1762336799, -7.4812, 33.4521, 1524.0, False,
     92.6, 186.3, -4.55, None, 1600.2, "2341", False, 0, 4],
    ["4CA7B4", "RYR8NP  ", "Ireland", 1762336790, 1762336795, -7.5901, 33.3672, None, True,
     0.0, 92.8, None, None, None, None, False, 0, 3],
]


# ---------- faux objets ----------

class FakeClient:
    """Remplace OpenSkyClient : renvoie une réponse fixe, ou lève une erreur."""

    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = 0

    def get_states(self, bbox):
        self.calls += 1
        if self.error is not None:
            raise self.error
        return self.response


class FakeSink:
    """Remplace Kafka : garde les messages en mémoire."""

    def __init__(self):
        self.sent = []  # liste de (topic, key, value)

    def send(self, topic, key, value):
        self.sent.append((topic, key, value))

    def flush(self, timeout=30):
        return 0


class RecordingStop(StopFlag):
    """StopFlag qui n'attend pas vraiment : il note les délais demandés.

    Il demande l'arrêt après `max_waits` attentes, pour que la boucle se termine.
    """

    def __init__(self, max_waits):
        super().__init__()
        self.max_waits = max_waits
        self.delays = []

    def wait(self, seconds):
        self.delays.append(seconds)
        if len(self.delays) >= self.max_waits:
            self.set()
        return self.is_set


def fixed_clock():
    return 0.0  # le temps « ne passe pas » : délai = intervalle exact


# ---------- FlightsTick ----------

def test_tick_publishes_every_plane_with_icao24_key():
    client = FakeClient(StatesResponse(time=SNAPSHOT, states=STATES, remaining_credits=3998))
    sink = FakeSink()
    tick = FlightsTick(client, sink, Settings())

    result = tick()

    assert result is None                                  # pas d'attente spéciale
    assert len(sink.sent) == 2
    assert {topic for topic, _, _ in sink.sent} == {"flights.raw"}
    keys = [key for _, key, _ in sink.sent]
    assert keys == ["020118", "4ca7b4"]                    # clé = icao24 en minuscules
    assert all(value["icao24"] == key for _, key, value in sink.sent)
    assert tick.total_messages == 2


def test_tick_returns_retry_delay_on_429():
    client = FakeClient(error=RateLimited(retry_after_s=1200))
    sink = FakeSink()
    tick = FlightsTick(client, sink, Settings())

    result = tick()

    assert result == 1200          # le runner attendra le délai imposé par OpenSky
    assert sink.sent == []         # rien n'est envoyé


def test_tick_with_no_plane_sends_nothing():
    client = FakeClient(StatesResponse(time=SNAPSHOT, states=[], remaining_credits=3998))
    sink = FakeSink()

    assert FlightsTick(client, sink, Settings())() is None
    assert sink.sent == []


# ---------- effective_interval ----------

def test_interval_is_raised_in_anonymous_mode():
    anonymous = Settings(flights_interval_s=45)   # pas d'identifiants → 400 crédits/jour

    assert effective_interval(anonymous) == 432   # 86 400 s / (400 // 2) appels


def test_interval_is_kept_when_authenticated():
    auth = Settings(opensky_client_id="id", opensky_client_secret="secret", flights_interval_s=45)

    assert effective_interval(auth) == 45


# ---------- run_loop ----------

def test_runner_backs_off_then_returns_to_normal_interval():
    results = [RuntimeError("réseau"), RuntimeError("réseau"), None]

    def tick():
        r = results.pop(0)
        if isinstance(r, Exception):
            raise r
        return r

    stop = RecordingStop(max_waits=3)
    run_loop(tick, interval_s=10, stop=stop, clock=fixed_clock)

    assert stop.delays == [10, 20, 10]   # 1er échec : 10 s, 2e : ×2 = 20 s, puis retour à 10 s


def test_runner_backoff_is_capped():
    def tick():
        raise RuntimeError("panne")

    stop = RecordingStop(max_waits=5)
    run_loop(tick, interval_s=100, stop=stop, max_backoff_s=300, clock=fixed_clock)

    assert stop.delays == [100, 200, 300, 300, 300]   # jamais plus de 300 s


def test_runner_uses_delay_returned_by_tick():
    stop = RecordingStop(max_waits=1)
    run_loop(lambda: 1200, interval_s=45, stop=stop, clock=fixed_clock)

    assert stop.delays == [1200]


def test_once_mode_runs_a_single_cycle():
    calls = []
    stop = RecordingStop(max_waits=10)

    run_loop(lambda: calls.append(1), interval_s=45, stop=stop, once=True, clock=fixed_clock)

    assert len(calls) == 1
    assert stop.delays == []   # aucune attente : on s'arrête tout de suite


def test_main_once_dry_run_makes_one_call(monkeypatch):
    client = FakeClient(StatesResponse(time=SNAPSHOT, states=STATES, remaining_credits=3998))
    monkeypatch.setattr(flights_producer, "build_client", lambda settings: client)
    monkeypatch.setattr(flights_producer, "load_dotenv", lambda: None)   # ne pas lire le vrai .env
    monkeypatch.delenv("OPENSKY_CLIENT_ID", raising=False)
    monkeypatch.delenv("OPENSKY_CLIENT_SECRET", raising=False)

    flights_producer.main(["--once", "--dry-run"])

    assert client.calls == 1

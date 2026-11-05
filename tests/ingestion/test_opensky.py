import pytest
import requests
import responses

from skywatch_ingestion.config import MOROCCO_BBOX
from skywatch_ingestion.opensky import OpenSkyClient, RateLimited, TokenManager

API = "https://opensky-network.org/api"
TOKEN_URL = "https://auth.example/token"
STATES_URL = f"{API}/states/all"

# Réponse réaliste de /states/all : un vol RAM en approche, un Ryanair au sol
PAYLOAD = {
    "time": 1762336800,
    "states": [
        ["020118", "RAM200  ", "Morocco", 1762336798, 1762336799, -7.4812, 33.4521, 1524.0, False,
         92.6, 186.3, -4.55, None, 1600.2, "2341", False, 0, 4],
        ["4ca7b4", "RYR8NP  ", "Ireland", 1762336790, 1762336795, -7.5901, 33.3672, None, True,
         0.0, 92.8, None, None, None, None, False, 0, 3],
    ],
}


class FakeClock:
    """Horloge contrôlable : on avance le temps sans attendre."""

    def __init__(self):
        self.t = 1000.0

    def __call__(self):
        return self.t


def mock_token(value="tok-1", expires=1800):
    responses.post(TOKEN_URL, json={"access_token": value, "expires_in": expires})


@responses.activate
def test_token_cached_then_refreshed_before_expiry():
    clock = FakeClock()
    mock_token("tok-1")
    mock_token("tok-2")
    tm = TokenManager("id", "secret", TOKEN_URL, clock=clock)

    assert tm.get_token() == "tok-1"
    clock.t += 1000                      # 1 000 s plus tard : jeton encore valide
    assert tm.get_token() == "tok-1"
    assert len(responses.calls) == 1     # aucun nouvel appel au serveur

    clock.t += 750                       # à moins de 60 s de l'expiration (1 800 s)
    assert tm.get_token() == "tok-2"     # renouvelé
    assert len(responses.calls) == 2


@responses.activate
def test_get_states_sends_bearer_and_reads_credits():
    mock_token()
    responses.get(STATES_URL, json=PAYLOAD, headers={"X-Rate-Limit-Remaining": "3838"})
    client = OpenSkyClient(API, TokenManager("id", "secret", TOKEN_URL))

    resp = client.get_states(MOROCCO_BBOX)

    req = responses.calls[1].request
    assert req.headers["Authorization"] == "Bearer tok-1"
    assert "lamin=28.0" in req.url and "lomax=-1.0" in req.url
    assert len(resp.states) == 2
    assert resp.remaining_credits == 3838


@responses.activate
def test_429_raises_with_retry_after():
    responses.get(STATES_URL, status=429, headers={"X-Rate-Limit-Retry-After-Seconds": "5400"})
    with pytest.raises(RateLimited) as exc:
        OpenSkyClient(API).get_states(MOROCCO_BBOX)
    assert exc.value.retry_after_s == 5400


@responses.activate
def test_401_refreshes_token_once():
    mock_token("old")
    mock_token("new")
    responses.get(STATES_URL, status=401)
    responses.get(STATES_URL, json=PAYLOAD)
    client = OpenSkyClient(API, TokenManager("id", "secret", TOKEN_URL))

    resp = client.get_states(MOROCCO_BBOX)

    assert len(resp.states) == 2
    assert responses.calls[-1].request.headers["Authorization"] == "Bearer new"


@responses.activate
def test_null_states_means_empty_sky():
    responses.get(STATES_URL, json={"time": 1, "states": None})
    assert OpenSkyClient(API).get_states(MOROCCO_BBOX).states == []


@responses.activate
def test_server_error_raises():
    responses.get(STATES_URL, status=503)
    with pytest.raises(requests.HTTPError):
        OpenSkyClient(API).get_states(MOROCCO_BBOX)

"""Client OpenSky : jeton OAuth2, appel /states/all, gestion du 429 et des crédits."""

from __future__ import annotations

import logging
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

import requests

from .config import BoundingBox, Settings

log = logging.getLogger(__name__)

# Ordre des champs d'un « state vector » renvoyé par /states/all.
STATE_FIELDS: tuple[str, ...] = (
    "icao24",
    "callsign",
    "origin_country",
    "time_position",
    "last_contact",
    "longitude",
    "latitude",
    "baro_altitude",
    "on_ground",
    "velocity",
    "true_track",
    "vertical_rate",
    "sensors",
    "geo_altitude",
    "squawk",
    "spi",
    "position_source",
    "category",  # présent seulement avec extended=1
)

HEADER_REMAINING = "X-Rate-Limit-Remaining"
HEADER_RETRY_AFTER = "X-Rate-Limit-Retry-After-Seconds"


class RateLimited(Exception):
    """Crédits épuisés : l'API a répondu 429."""

    def __init__(self, retry_after_s: float):
        super().__init__(f"OpenSky 429, nouvel essai dans {retry_after_s:.0f} s")
        self.retry_after_s = retry_after_s


class TokenManager:
    """Obtient et met en cache un jeton OAuth2 (client credentials).

    Le jeton OpenSky dure environ 30 minutes ; on le renouvelle 60 s avant
    l'expiration pour ne jamais envoyer un jeton périmé.
    """

    REFRESH_MARGIN_S = 60

    def __init__(
        self,
        client_id: str,
        client_secret: str,
        token_url: str,
        session: requests.Session | None = None,
        clock: Callable[[], float] = time.monotonic,
    ):
        self._client_id = client_id
        self._client_secret = client_secret
        self._token_url = token_url
        self._session = session or requests.Session()
        self._clock = clock
        self._token: str | None = None
        self._expires_at = 0.0

    def get_token(self) -> str:
        if self._token is None or self._clock() >= self._expires_at - self.REFRESH_MARGIN_S:
            self._refresh()
        assert self._token is not None
        return self._token

    def invalidate(self) -> None:
        self._token = None

    def _refresh(self) -> None:
        resp = self._session.post(
            self._token_url,
            data={
                "grant_type": "client_credentials",
                "client_id": self._client_id,
                "client_secret": self._client_secret,
            },
            timeout=15,
        )
        resp.raise_for_status()
        payload = resp.json()
        self._token = payload["access_token"]
        self._expires_at = self._clock() + float(payload.get("expires_in", 1800))
        log.info("Jeton OpenSky renouvelé (valide %s s)", payload.get("expires_in", 1800))


@dataclass
class StatesResponse:
    time: int
    states: list[list[Any]]
    remaining_credits: int | None


class OpenSkyClient:
    def __init__(
        self,
        api_url: str,
        token_manager: TokenManager | None = None,
        session: requests.Session | None = None,
    ):
        self._api_url = api_url.rstrip("/")
        self._tokens = token_manager
        self._session = session or requests.Session()

    def get_states(self, bbox: BoundingBox) -> StatesResponse:
        """Appelle /states/all sur la zone. Lève RateLimited sur un 429."""
        resp = self._request(bbox)
        if resp.status_code == 401 and self._tokens is not None:
            # Jeton refusé par le serveur : on en redemande un, une seule fois.
            log.warning("401 reçu, renouvellement du jeton")
            self._tokens.invalidate()
            resp = self._request(bbox)

        remaining = _int_header(resp, HEADER_REMAINING)
        if resp.status_code == 429:
            retry_after = _int_header(resp, HEADER_RETRY_AFTER)
            raise RateLimited(float(retry_after) if retry_after is not None else 3600.0)
        resp.raise_for_status()

        payload = resp.json() or {}
        return StatesResponse(
            time=int(payload.get("time") or 0),
            states=payload.get("states") or [],  # null quand aucun avion
            remaining_credits=remaining,
        )

    def _request(self, bbox: BoundingBox) -> requests.Response:
        headers = {}
        if self._tokens is not None:
            headers["Authorization"] = f"Bearer {self._tokens.get_token()}"
        return self._session.get(
            f"{self._api_url}/states/all",
            params={**bbox.as_params(), "extended": 1},
            headers=headers,
            timeout=20,
        )


def _int_header(resp: requests.Response, name: str) -> int | None:
    raw = resp.headers.get(name)
    try:
        return int(raw) if raw is not None else None
    except ValueError:
        return None


def build_client(settings: Settings) -> OpenSkyClient:
    """Client authentifié si les identifiants sont présents, anonyme sinon."""
    tokens = None
    if settings.authenticated:
        tokens = TokenManager(
            settings.opensky_client_id,  # type: ignore[arg-type]
            settings.opensky_client_secret,  # type: ignore[arg-type]
            settings.opensky_token_url,
        )
    return OpenSkyClient(settings.opensky_api_url, token_manager=tokens)

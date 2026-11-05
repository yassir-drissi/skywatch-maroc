import pytest

from skywatch_ingestion.config import (
    MOROCCO_BBOX,
    BoundingBox,
    Settings,
    daily_credit_usage,
    min_safe_interval,
    opensky_credit_cost,
)


def test_morocco_bbox_costs_two_credits():
    assert MOROCCO_BBOX.area_sq_deg == 96
    assert opensky_credit_cost(MOROCCO_BBOX.area_sq_deg) == 2


@pytest.mark.parametrize("area,cost", [(10, 1), (25, 1), (26, 2), (100, 2), (101, 3), (400, 3), (401, 4)])
def test_credit_cost_brackets(area, cost):
    assert opensky_credit_cost(area) == cost


def test_45s_interval_fits_daily_budget():
    used = daily_credit_usage(MOROCCO_BBOX, 45)
    assert used == 3840
    assert used <= 4000


def test_min_safe_interval_anonymous():
    # 400 crédits / 2 = 200 appels par jour → un appel toutes les 432 s
    assert min_safe_interval(MOROCCO_BBOX, 400) == pytest.approx(432)


def test_invalid_bbox_rejected():
    with pytest.raises(ValueError):
        BoundingBox(lamin=36, lomin=-13, lamax=28, lomax=-1)


def test_bbox_contains():
    assert MOROCCO_BBOX.contains(33.37, -7.59)      # Casablanca
    assert not MOROCCO_BBOX.contains(48.85, 2.35)   # Paris
    assert not MOROCCO_BBOX.contains(None, -7.0)


def test_settings_from_env(monkeypatch):
    monkeypatch.setenv("OPENSKY_CLIENT_ID", "id")
    monkeypatch.setenv("OPENSKY_CLIENT_SECRET", "secret")
    monkeypatch.setenv("FLIGHTS_POLL_INTERVAL_S", "60")
    s = Settings.from_env()
    assert s.authenticated and s.daily_credit_limit == 4000
    assert s.flights_interval_s == 60


def test_settings_anonymous(monkeypatch):
    monkeypatch.delenv("OPENSKY_CLIENT_ID", raising=False)
    monkeypatch.delenv("OPENSKY_CLIENT_SECRET", raising=False)
    s = Settings.from_env()
    assert not s.authenticated and s.daily_credit_limit == 400
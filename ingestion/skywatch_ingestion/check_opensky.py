"""Vérifie tes identifiants OpenSky et la zone Maroc, sans Kafka.

    python -m skywatch_ingestion.check_opensky
"""

from __future__ import annotations

import sys

from dotenv import load_dotenv

from .config import Settings, opensky_credit_cost
from .opensky import RateLimited, build_client


def main() -> int:
    load_dotenv()  # lit le fichier .env à la racine du projet
    settings = Settings.from_env()
    print(f"Authentification : {'OAuth2' if settings.authenticated else 'ANONYME (400 crédits/jour)'}")
    area = settings.bbox.area_sq_deg
    print(f"Zone : {area:.0f} deg², {opensky_credit_cost(area)} crédits par appel")

    try:
        resp = build_client(settings).get_states(settings.bbox)
    except RateLimited as exc:
        print(f"Crédits épuisés : {exc}")
        return 2
    except Exception as exc:  # noqa: BLE001
        print(f"ÉCHEC : {exc}")
        return 1

    # Un « state » est une liste : [0]=icao24, [1]=indicatif, [5]=longitude, [6]=latitude, [7]=altitude, [8]=au sol
    en_vol = [s for s in resp.states if not s[8]]
    print(f"OK : {len(resp.states)} avions ({len(en_vol)} en vol), crédits restants : {resp.remaining_credits}")
    for s in en_vol[:5]:
        indicatif = (s[1] or "?").strip()
        print(f"  {indicatif:<9} {s[0]}  lat={s[6]}  lon={s[5]}  alt={s[7]} m")
    return 0


if __name__ == "__main__":
    sys.exit(main())

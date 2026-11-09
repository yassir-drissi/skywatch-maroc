# SkyWatch Maroc — Passation

| Partie | Semaines | Auteur | Date |
|---|---|---|---|
| **A** | 3 : lakehouse Silver / Gold (Delta Lake, Databricks) | Mustapha Hamdaoui Alaoui | 9 novembre 2026 |
| **B** | 1 et 2 : ingestion Kafka, streaming Spark Scala | Mohamed Benheddane (suite de Yassir Drissi) | 6 novembre 2026 |

---

# Partie A — Semaine 3 : Silver et Gold en Delta Lake

## A1. Résumé

La semaine 3 est terminée et testée avec de vraies données, **en local et sur Databricks**.

- **Silver** : une position d'avion propre par ligne (démo et doublons retirés, aéroport le plus proche,
  hauteur au-dessus de la piste) + la météo propre.
- **Gold** : `gold_flights` (un vol par ligne : arrivée, départ ou survol), `gold_airport_hourly`
  (trafic par aéroport et par heure, croisé avec la météo), `gold_anomalies` (alertes du streaming
  reliées au vol et à la météo).
- **Contrôles qualité avant écriture** : si une règle échoue, le job s'arrête et Gold n'est pas modifié.
- **Écriture par `MERGE INTO`** : relancer un job ne crée jamais de doublons (vérifié : `DESCRIBE HISTORY`).
- **Même code partout** : PySpark + SQL dans `databricks/`, lancé en local par Docker (service `lakehouse`)
  et sur Databricks Free Edition par un job planifié `silver → gold` (calcul serverless).
- Le streaming Scala archive maintenant aussi les alertes : 6e query `bronze_alerts` → `bronze/alerts`.
- Tests : **16 tests lakehouse** (vrai Spark local, sans Delta) + les 55 Python et 39 Scala existants.
  CI : nouveau job `lakehouse`.

## A2. Ce qui a été fait

| Commit (message) | Contenu |
|---|---|
| `feat(lakehouse): couche Silver PySpark + Delta (MERGE idempotent)` | `silver.py`, `airports.py`, `lake.py`, `run_silver.py`, service Docker `lakehouse`, 10 tests |
| `feat(lakehouse): couche Gold (...) + contrôles qualité + CI` | `gold.py`, `quality.py`, `run_gold.py`, query `bronze_alerts` (Scala), job CI `lakehouse`, 6 tests |
| `feat(lakehouse): compatible Databricks serverless (MERGE SQL, cache optionnel)` | `MERGE INTO` en SQL, `cache()` toléré absent |
| `fix(lakehouse): jobs lançables sur Databricks serverless (sans __file__)` | `_lakehouse_dir()` dans les deux jobs |
| `fix(lakehouse): dossier Bronze vide détecté tout de suite sur Spark Connect` | `df.columns` dans `read_bronze` |
| `docs: passation semaine 3, README, script de synchronisation` | ce document, README, `scripts/sync_bronze_databricks.ps1` |

## A3. Architecture du lakehouse

```
streaming Scala (semaine 2)                 batch PySpark (semaine 3, dossier databricks/)
─────────────────────────────               ───────────────────────────────────────────────
bronze/flights  (Parquet) ──┐
bronze/weather  (Parquet) ──┼─► run_silver.py ─► silver_flights, silver_weather   (Delta, MERGE)
bronze/alerts   (Parquet) ──┘                          │
                     │                                 ▼
                     └──────────────────────► run_gold.py ─► gold_flights        (Delta, MERGE)
                                               contrôles     gold_airport_hourly (Delta, réécrite)
                                               qualité       gold_anomalies      (Delta, MERGE)
```

| Fichier | Rôle |
|---|---|
| `databricks/skywatch_lakehouse/airports.py` | 6 aéroports + altitude des pistes (mêmes valeurs que `Airports.scala`, vérifié par un test) |
| `databricks/skywatch_lakehouse/silver.py` | Bronze → Silver : schémas explicites, filtres, dédoublonnage, aéroport le plus proche |
| `databricks/skywatch_lakehouse/gold.py` | Silver → Gold : découpage en vols, classement, agrégats horaires, anomalies |
| `databricks/skywatch_lakehouse/quality.py` | Contrôles : clés uniques, valeurs non nulles, pas de démo, types de vol connus |
| `databricks/skywatch_lakehouse/lake.py` | Où lire / écrire : dossiers Delta en local, tables Unity Catalog sur Databricks ; `MERGE INTO` |
| `databricks/jobs/run_silver.py`, `run_gold.py` | Points d'entrée (local et Databricks), paramètres `--lake-path`, `--schema` |
| `scripts/sync_bronze_databricks.ps1` | Envoie Bronze (sans `_spark_metadata`) vers le volume Databricks, lance le job |

## A4. Les tables

**`silver_flights`** (clé `icao24, snapshot_time`, partition `event_date`) : positions OpenSky seulement
(`source = "opensky"`), identifiant valide, coordonnées dans la zone 28–36°N × 13–1°W, une ligne par
(avion, instantané) en gardant le message arrivé en dernier dans Kafka. Colonnes ajoutées : `event_time`,
`callsign` nettoyé, `airline_icao` (RAM200 → RAM), `altitude_m` (baro, sinon géométrique ; aberrations
retirées), `velocity_kmh`, `nearest_airport`, `distance_km`, `agl_m` (hauteur au-dessus de la piste),
`in_terminal_area` (≤ 60 km).

**`silver_weather`** (clé `icao, observed_at`) : relevés Open-Meteo, `low_visibility` (< 5 km),
`strong_wind` (vent ≥ 20 kt ou rafales ≥ 30 kt).

**`gold_flights`** (clé `flight_id = icao24_premierInstantané`) : un vol = positions d'un même avion sans
trou de plus de 30 min. Classement à partir des positions près d'un aéroport :

| `flight_type` | Règle |
|---|---|
| `OVERFLIGHT` | jamais sous 1 500 m au-dessus d'une piste à moins de 60 km |
| `ARRIVAL` | vu au moins 300 m plus haut au début qu'à la fin (descend vers la piste) |
| `DEPARTURE` | vu au moins 300 m plus bas au début qu'à la fin (monte) |
| `LOW_LEVEL` | bas près d'un aéroport, sans montée ni descente nette |

**`gold_airport_hourly`** (clé `airport, date, hour`) : avions, positions, avions bas, arrivées, départs,
et météo de l'heure (pire visibilité, vent et rafales max, pluie). Les heures sans trafic sont gardées
(utile pour comparer trafic et météo).

**`gold_anomalies`** (clé `alert_type, icao24, detected_at`) : alertes HOLDING / GO_AROUND, reliées au
`flight_id` et à la météo de l'heure. Les alertes des scripts de démo (`fff001`, `fff002`) sont gardées
avec `is_demo = true` (filtrer dessus pour les analyses).

## A5. Lancer en local (Windows · PowerShell)

Le streaming doit tourner (Partie B, §3). Puis :

```powershell
docker compose --profile batch run --rm lakehouse jobs/run_silver.py
docker compose --profile batch run --rm lakehouse jobs/run_gold.py
```

- Le service `lakehouse` (profil `batch`) ne démarre pas avec `docker compose up` : on le lance à la demande.
- Il monte `./data` au **même chemin** que `spark-streaming` (`/opt/skywatch/data`) : Bronze contient
  `_spark_metadata` avec des chemins absolus, un autre chemin donnerait 0 ligne.
- Résultat : dossiers Delta `data/lake/silver/*` et `data/lake/gold/*` (avec `_delta_log`).
- Logs lisibles : `chcp 65001` avant, ou `... *> data\gold.log` puis
  `Select-String -Path data\gold.log -Encoding UTF8 -Pattern "INFO run_|skywatch.lake|ERROR"`.

**Tests** — PySpark ne marche pas bien directement sous Windows (voir A9) ; on les lance dans l'image Spark :

```powershell
docker run --rm -u 0 -v "${PWD}:/app" -w /app `
  -e PYTHONPATH=/opt/spark/python:/opt/spark/python/lib/py4j-0.10.9.7-src.zip:/app/databricks:/app/ingestion:/app/tests/lakehouse `
  -e PYSPARK_PYTHON=python3 spark:3.5.7-scala2.12-java17-python3-ubuntu `
  bash -c "pip install -q pytest==8.3.4 && python3 -m pytest tests/lakehouse -q -p no:cacheprovider"
```

## A6. Databricks (Free Edition)

Mise en place (une fois) :

1. CLI : `winget install Databricks.DatabricksCLI`, puis
   `databricks auth login --host https://<workspace>.cloud.databricks.com` (connexion dans le navigateur,
   rien n'est écrit dans le projet).
2. SQL Editor (warehouse **Serverless Starter Warehouse**, pas Lakebase Postgres) :
   ```sql
   CREATE SCHEMA IF NOT EXISTS workspace.skywatch;
   CREATE VOLUME IF NOT EXISTS workspace.skywatch.lake;
   ```
3. Envoyer Bronze : `.\scripts\sync_bronze_databricks.ps1`
4. Code : **Workspace → Create → Git folder** sur ce repo, branche voulue. Penser au **Pull** après chaque
   push (ou mettre les tâches en *Source : Git provider* pour toujours prendre le dernier commit).
5. Job `skywatch-lakehouse` (Jobs & Pipelines) : tâche `silver` puis tâche `gold` (*depends on* silver),
   type **Python script**, calcul **Serverless**, paramètres (liste, pas clé/valeur) :
   `["--lake-path","/Volumes/workspace/skywatch/lake","--schema","workspace.skywatch"]`
6. Planning : *Schedules & Triggers → Scheduled*, toutes les 6 h ; *Retries* = 1.

Tables créées : `workspace.skywatch.silver_flights`, `silver_weather`, `gold_flights`,
`gold_airport_hourly` (et `gold_anomalies` dès qu'une alerte est archivée).

```sql
SELECT flight_type, airport, COUNT(*) AS vols FROM workspace.skywatch.gold_flights GROUP BY ALL ORDER BY vols DESC;
DESCRIBE HISTORY workspace.skywatch.silver_flights;   -- version 0 = création, puis un MERGE par exécution
```

**Ce qui n'est pas automatique :** Bronze vit sur le PC (Kafka et le streaming tournent dans Docker).
Databricks ne voit que ce qu'on y envoie avec le script. Un flux continu demanderait Kafka dans le cloud.

## A7. Décisions techniques

| Sujet | Choix | Pourquoi |
|---|---|---|
| Langage | PySpark + SQL (le streaming reste en Scala) | Databricks Free Edition ne lance pas de Scala |
| Transformations | Fonctions pures `DataFrame → DataFrame` | Testables avec un Spark local, sans Delta ni Databricks |
| Un seul code | `Lake(lake_path, schema)` | Local = dossiers Delta ; Databricks = tables Unity Catalog ; seul `--schema` change |
| Écriture | `MERGE INTO` (SQL) sur une clé | Idempotent ; la même requête marche en local et en serverless |
| `gold_airport_hourly` | Réécrite entièrement | Petite table d'agrégats : plus simple qu'un MERGE |
| Qualité | Contrôles **avant** écriture | Ne jamais publier une table Gold fausse |
| Démo | Retirée de Silver ; gardée et marquée dans `gold_anomalies` | Analyses propres, démo toujours possible |
| Vol | Trou > 30 min = nouveau vol | OpenSky donne une position toutes les ~45 s quand l'avion est couvert |
| Seuils | 1 500 m AGL, 300 m de montée/descente, 60 km | Constantes en haut de `gold.py` |
| Bronze sur Databricks | Copie sans `_spark_metadata` | Chemins absolus Docker inutilisables ailleurs |

## A8. Ce que disent les vraies données (9 novembre, ~12 h – 15 h)

| Mesure | Valeur |
|---|---|
| Positions Silver | 4 924 (0 démo, 0 position invalide, 0 doublon : le producteur n'a pas redémarré) |
| Vols reconstruits | 283 |
| Survols (`OVERFLIGHT`) | 275 (97 %) : le Maroc est surtout un couloir Europe ↔ Canaries / Afrique de l'Ouest |
| Arrivées / départs / bas | 4 / 3 / 1, **tous à Tanger (GMTT)** |
| Casablanca, Rabat, Fès | 0 arrivée : les « avions dans la zone » de Silver sont des survols à haute altitude |
| Databricks | job `silver` 28 s, `gold` 32 s (serverless) |

Gold **mesure** la limite déjà vue en semaine 2 : OpenSky dépend d'antennes ADS-B bénévoles, nombreuses
dans le sud de l'Espagne et rares au Maroc. Les avions qui descendent vers Casablanca sortent de la
couverture avant d'atterrir.

## A9. Pièges rencontrés et solutions

| Symptôme | Cause | Solution |
|---|---|---|
| Tests PySpark sous Windows : `o48.parquet`, `collectToPython` en échec | Spark sous Windows : pas de `winutils.exe`, workers Python instables | Lancer les tests dans l'image Docker Spark (A5) |
| `PYTHON_VERSION_MISMATCH` | Workers Spark sur un autre Python que pytest | `PYSPARK_PYTHON = sys.executable` dans les tests |
| `no tests ran` | Fichiers de test vides (pièce jointe pas enregistrée) | Vérifier la taille : `Get-ChildItem tests\lakehouse` |
| `W292 No newline at end of file` en CI | Code collé sans saut de ligne final | `ruff check . --fix` avant chaque commit ; **Files: Insert Final Newline** |
| Databricks : `NameError: __file__` | Le serverless lance le script avec `exec()` | `_lakehouse_dir()` essaie `__file__`, `sys.argv[0]`, puis le dossier courant |
| Databricks : encore l'ancienne erreur après correction | Git folder pas mis à jour | **Pull** dans le Git folder (ou source Git provider) |
| Databricks : `UNABLE_TO_INFER_SCHEMA` sur `bronze/alerts` | Dossier vide ; Spark Connect ne lit qu'au premier calcul, hors du `try` | `df.columns` juste après la lecture |
| Databricks : `cache()` refusé | Pas de cache en serverless | `lake.cache()` tolère l'absence |
| Logs avec `├⌐` | Encodage de la console PowerShell | `chcp 65001` ; `Select-String -Encoding UTF8` |

## A10. Suite (semaine 4 et après)

- **Airflow** : orchestrer `sync_bronze_databricks.ps1` (ou son équivalent) puis le job Databricks.
- **Contrôles de fraîcheur** : détecter les trous de données (ex. 17 h 45 – 19 h 50 le 6 novembre) :
  heures sans aucune position dans `gold_airport_hourly`.
- **`gold_anomalies`** : lancer `demo_holding` / `demo_go_around` pour la remplir et la montrer.
- Après fusion de la PR : passer le Git folder et le job sur la branche `main`.
- Snowflake + dbt (semaine 5) : partir de `gold_flights` et `gold_airport_hourly`.

---

# Partie B — Semaines 1 et 2 (ingestion + streaming)

**Auteur :** Mohamed Benheddane · **Date :** 6 novembre 2026
**Suite de :** dossier de passation semaine 1 de Yassir Drissi (ingestion OpenSky → Kafka)

## 1. Résumé

La fin de la semaine 1 et toute la semaine 2 sont terminées et testées avec de vraies données.

- Les deux producteurs (vols OpenSky toutes les 45 s, météo Open-Meteo toutes les heures) tournent dans Docker.
- Un job **Spark Structured Streaming en Scala** lit Kafka en continu et lance 5 queries :
  archive Bronze des vols et de la météo, trafic par aéroport toutes les 5 minutes,
  détection des **attentes en circuit** et des **remises de gaz** vers le topic `flights.alerts`.
- Tests : **55 tests Python** (pytest) et **39 tests Scala** (ScalaTest), tous verts.
- CI GitHub Actions : un job Python (ruff, pytest, docker compose config) et un job Scala (sbt test + package).

## 2. Ce qui a été fait

| Étape | Commit | Contenu |
|---|---|---|
| — | `857f402` | `confluent-kafka` 2.16.1 : roues précompilées pour Python 3.14 sous Windows |
| 6b | `eed2d49` | Tests du producteur (10), `ingestion/Dockerfile` (non root), service `flights-producer` |
| 6c | `984f8db` | Producteur météo Open-Meteo → `weather.raw` (un appel pour les 6 aéroports), schéma JSON, 9 tests |
| 7 | `972d1ef` | CI GitHub Actions + badge dans le README |
| 8a | `8a80f2a` | Projet sbt (Scala 2.12.20), `Geo` (haversine, `headingDelta`), `Airports`, 9 tests |
| 8b | `83c5db7` | Spark 3.5.7 dans Docker, `BronzeJob` : Kafka → Parquet `data/lake/bronze/<table>/ingest_date=…` |
| 8c | `ec12206` | Parsing typé de `flights.raw`, watermark 10 min, dédoublonnage, trafic par aéroport / 5 min |
| 8d | `23441d5` | `HoldingDetector` + `HoldingJob` (état par avion) → alertes `HOLDING`, script `demo_holding` |
| 8e | `f2e200e` | `GoAroundDetector` + `GoAroundJob` → alertes `GO_AROUND`, altitude des pistes, script `demo_go_around` |
| 8f | `ab26bd9` | Schéma `flights_alerts.schema.json`, job Scala dans la CI, README, ce document |

### Branches et Pull Requests

Le travail est livré en **2 Pull Requests**, une par semaine :

| PR | Branche | Base | Contenu |
|---|---|---|---|
| 1 | `feat/ci` | `main` | Fin de la semaine 1 : producteur dans Docker, météo, CI (4 commits, Python) |
| 2 | `feat/docs-semaine2` | `feat/ci`, puis `main` | Semaine 2 : Spark Scala, détections, passation (7 commits) |

La PR 2 part de la PR 1. Pour la relire, sa base est d'abord `feat/ci` : GitHub ne montre alors
que le travail de la semaine 2.

Ordre de fusion :

1. Fusionner la PR 1 dans `main` avec **« Create a merge commit »** (pas « Squash and merge »).
2. Dans la PR 2, bouton **Edit** à côté du titre : changer la base de `feat/ci` en `main`.
3. Fusionner la PR 2 avec **« Create a merge commit »**.

Chaque commit correspond à une étape du tableau ci-dessus : l'onglet « Commits » d'une PR
permet de relire étape par étape.

## 3. Démarrer sur sa machine (Windows · PowerShell)

Prérequis : Docker Desktop, Python 3.12+ (3.14 fonctionne), Java 17+ (21 fonctionne), sbt.

```powershell
git clone https://github.com/yassir-drissi/skywatch-maroc.git
cd skywatch-maroc
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements-dev.txt
pip install -r ingestion/requirements.txt
pytest                                  # 55 passed
sbt streaming/test                      # 39 tests (1re fois : 5 min de téléchargements)

Copy-Item .env.example .env             # identifiants OpenSky personnels
sbt streaming/package                   # jar : streaming/target/scala-2.12/skywatch-streaming_2.12-0.1.0.jar
docker compose up -d --build
docker compose logs -f spark-streaming
```

- Kafka UI : http://localhost:8080 · Spark UI : http://localhost:4040
- Après chaque modification du code Scala : `sbt streaming/package` puis `docker compose restart spark-streaming`.
- Les données (`data/`) et les checkpoints Spark restent sur le PC ; ils ne sont pas dans Git.

## 4. Comment fonctionne le job Spark

Point d'entrée : `streaming/src/main/scala/ma/skywatch/streaming/StreamingApp.scala`.
Une seule application Spark (moins de mémoire) qui lance 5 queries :

| Query | Entrée | Traitement | Sortie |
|---|---|---|---|
| `bronze_flights` | `flights.raw` | copie brute + métadonnées Kafka | `data/lake/bronze/flights` (Parquet) |
| `bronze_weather` | `weather.raw` | copie brute | `data/lake/bronze/weather` (Parquet) |
| `traffic_5min` | `flights.raw` | parsing, watermark 10 min, doublons, fenêtres de 5 min | console (logs) |
| `holding_alerts` | `flights.raw` | état par avion, 10 dernières minutes | `flights.alerts` (`HOLDING`) |
| `go_around_alerts` | `flights.raw` | état par avion, 5 dernières minutes | `flights.alerts` (`GO_AROUND`) |

| Fichier | Rôle |
|---|---|
| `JobConfig` | Configuration par variables d'environnement (Kafka, `LAKE_PATH`, topics, trigger) |
| `BronzeJob` | `readTopic` (options Kafka communes) et écriture Bronze |
| `FlightsStream` | Schéma typé de `flights.raw`, `event_time`, watermark, dédoublonnage |
| `TrafficJob` | Trafic par aéroport : avions dans 60 km, positions sous 3 000 m |
| `HoldingDetector` / `HoldingJob` | Règle des attentes (Scala pur) / branchement `flatMapGroupsWithState` |
| `GoAroundDetector` / `GoAroundJob` | Règle des remises de gaz (Scala pur) / branchement Spark |
| `Geo`, `Airports` | Distances, virages, 6 aéroports avec l'altitude des pistes |

### Règles de détection

**Attente en circuit (`HOLDING`)** — sur les 10 dernières minutes d'un avion :
somme des virages ≥ 360° dans le même sens, altitude ≤ 6 000 m, pas au sol, à moins de 60 km
d'un aéroport. Un virage entre deux positions séparées de plus de 60 s n'est pas compté.

**Remise de gaz (`GO_AROUND`)** — sur les 5 dernières minutes :
point le plus bas à moins de 600 m au-dessus de la piste et à moins de 15 km de l'aéroport,
descente d'au moins 150 m avant, remontée d'au moins 300 m après, **aucune position au sol**
(sinon c'est un atterrissage).

Les deux : au plus une alerte toutes les 15 minutes par avion. Tous les seuils sont des
constantes en haut de `HoldingDetector` et `GoAroundDetector`.

## 5. Décisions techniques

| Sujet | Choix | Pourquoi |
|---|---|---|
| Versions | Scala 2.12.20, Spark 3.5.7 | Mêmes versions que Databricks (semaine 3) |
| Spark | Image officielle `spark:3.5.7-scala2.12-java17-ubuntu`, `local[2]` | Pas d'installation de Spark sous Windows |
| Jar | Spark en `Provided`, connecteur Kafka via `--packages` | Jar léger (≈ 95 Ko), seulement notre code |
| Une seule application | 5 queries dans `StreamingApp` | Une seule JVM : moins de mémoire |
| Bronze | JSON brut + métadonnées Kafka, partition par `ingest_date` | On peut tout rejouer si une règle Silver est fausse |
| Trigger | 1 minute | Évite des milliers de petits fichiers Parquet |
| Event time | `snapshot_time` (pas `ingested_at`) | La position correspond au moment de l'instantané OpenSky |
| Watermark | 10 minutes | Accepte un peu de retard, libère la mémoire ensuite |
| Détection | Règles en Scala pur, testées sans Spark | Tests rapides, trajectoires inventées faciles à écrire |
| État | Une query et un état par type d'alerte | Ajouter une détection ne casse pas les checkpoints existants |
| Altitude | Hauteur au-dessus de la piste = altitude − `elevationM` | Fès est à 579 m : sans ça, la règle serait fausse |
| Démo | `demo_holding`, `demo_go_around`, `source = "demo"` | Montrer les alertes ; la couche Silver doit filtrer `source = "demo"` |

## 6. Ce que disent les vraies données (6 novembre, 17 h – 21 h 30)

| Aéroport | Positions sous 3 000 m par tranche de 5 min | Conclusion |
|---|---|---|
| GMTT Tanger | souvent 5 à 12 | Les approches sont visibles (antennes du sud de l'Espagne) |
| GMMN Casablanca, GMME Rabat, GMFF Fès | 0 | Seulement des survols à 9 000 – 11 000 m |
| GMMX Marrakech, GMAD Agadir | aucun avion dans 60 km | Pas de couverture |

**Conséquence :** les attentes et les remises de gaz ne sont détectables, en pratique, qu'à Tanger.
C'est une limite de la source gratuite, pas du code. Aucune vraie alerte observée ce soir ;
les deux types d'alerte ont été vérifiés de bout en bout avec les scripts de démo.

On voit aussi un trou de données entre 17 h 45 et 19 h 50 (producteurs arrêtés). Les contrôles
qualité de la semaine 4 devront détecter ce genre de trou.

## 7. Pièges rencontrés et solutions

| Symptôme | Cause | Solution |
|---|---|---|
| `Microsoft Visual C++ 14.0 or greater is required` | Python 3.14 et `confluent-kafka` 2.6.1 (pas de roue 3.14) | `confluent-kafka==2.16.1` |
| `sbt` : `[success]` mais aucun test | Fichier de test collé mais pas enregistré (0 octet) | Ctrl+S ; activer **File → Auto Save** |
| `ImportError` / `expected class or object definition` | Code collé dans le mauvais fichier (Python dans un `.scala`) | Vérifier le nom de l'onglet et l'extension avant de coller |
| `W292 No newline at end of file` | Pas de saut de ligne final | `ruff check . --fix` ; activer **Files: Insert Final Newline** |
| Fichiers « modifiés » sans changement de code | Fins de ligne passées de LF à CRLF | `git checkout -- <fichier>` pour annuler ; `git config core.autocrlf true` |
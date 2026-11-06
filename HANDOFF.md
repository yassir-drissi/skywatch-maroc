# SkyWatch Maroc — Passation (fin de semaine 1 + semaine 2)

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
| Fichiers « modifiés » sans changement de code | Fins de ligne passées de LF à CRLF | `git checkout --
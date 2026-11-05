#!/usr/bin/env bash
# Crée les topics SkyWatch. --if-not-exists : on peut le relancer sans erreur.
set -euo pipefail

BOOTSTRAP="${BOOTSTRAP:-localhost:9092}"
KT=/opt/kafka/bin/kafka-topics.sh

create() {
  local name=$1 partitions=$2 retention_ms=$3
  "$KT" --bootstrap-server "$BOOTSTRAP" --create --if-not-exists \
    --topic "$name" --partitions "$partitions" --replication-factor 1 \
    --config retention.ms="$retention_ms"
  echo "topic prêt : $name ($partitions partitions)"
}

create flights.raw    3 604800000    # positions des avions, gardées 7 jours
create weather.raw    1 2592000000   # météo, gardée 30 jours
create flights.alerts 1 2592000000   # alertes produites par Spark (semaine 2)

"$KT" --bootstrap-server "$BOOTSTRAP" --list
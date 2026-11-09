# Envoie Bronze (data/lake/bronze) vers le volume Databricks, puis lance le job si on le demande.
#
#   .\scripts\sync_bronze_databricks.ps1                  # envoi seulement
#   .\scripts\sync_bronze_databricks.ps1 -JobId 123456    # envoi + lancement du job skywatch-lakehouse
#
# Prérequis : CLI Databricks connectée (databricks auth login --host https://<workspace>.cloud.databricks.com)
#
# Pourquoi une copie intermédiaire ? Bronze contient _spark_metadata, écrit par le streaming avec des
# chemins absolus Docker (/opt/skywatch/...). Sur Databricks ces chemins n'existent pas : Spark lirait
# 0 ligne. On copie donc Bronze SANS _spark_metadata dans data/upload (ignoré par git), puis on envoie.

param(
    [string]$Volume = "dbfs:/Volumes/workspace/skywatch/lake",
    [string]$JobId = ""
)

$ErrorActionPreference = "Stop"
$source = "data\lake\bronze"
$staging = "data\upload\bronze"

if (-not (Test-Path $source)) { throw "$source introuvable : lancer d'abord le streaming (docker compose up -d)" }

Write-Host "1/3 Copie de Bronze sans _spark_metadata -> $staging"
# /MIR : copie miroir (supprime aussi de data/upload ce qui n'existe plus dans Bronze)
robocopy $source $staging /MIR /XD _spark_metadata /XF *.crc /NFL /NDL /NJH /NJS | Out-Null
if ($LASTEXITCODE -ge 8) { throw "robocopy a echoue (code $LASTEXITCODE)" }   # 0 a 7 = succes pour robocopy
$global:LASTEXITCODE = 0
$files = (Get-ChildItem $staging -Recurse -Filter *.parquet | Measure-Object).Count
Write-Host "    $files fichiers Parquet prets"

Write-Host "2/3 Envoi vers $Volume/bronze"
databricks fs cp -r $staging "$Volume/bronze" --overwrite | Out-Null
if ($LASTEXITCODE -ne 0) { throw "databricks fs cp a echoue : la CLI est-elle connectee ? (databricks current-user me)" }

if ($JobId) {
    Write-Host "3/3 Lancement du job $JobId (Silver -> Gold)"
    databricks jobs run-now $JobId
} else {
    Write-Host "3/3 Job non lance (ajouter -JobId <id> ; l'id est dans l'URL du job sur Databricks)"
}
Write-Host "Termine."
"""Où lire et où écrire : même code en local (dossiers) et sur Databricks (tables Unity Catalog).

  - Bronze est toujours lu comme des fichiers Parquet : <lake_path>/bronze/<table>
  - Silver et Gold sont des tables Delta :
      * en local      : dossiers <lake_path>/silver/<table> et <lake_path>/gold/<table>
      * sur Databricks : tables <catalog>.<schema>.<layer>_<table> (si `schema` est donné)

Écriture par MERGE (upsert) : relancer un job ne crée jamais de doublons (idempotent).
"""

from __future__ import annotations

import logging
from dataclasses import dataclass

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

log = logging.getLogger("skywatch.lake")


@dataclass(frozen=True)
class Lake:
    lake_path: str                 # ex. /opt/skywatch/data/lake  ou  /Volumes/skywatch/lakehouse/lake
    schema: str | None = None      # ex. skywatch.lakehouse  -> tables Unity Catalog (Databricks)

    # -- Bronze (Parquet écrit par le streaming Scala) ----------------------
    def read_bronze(self, spark: SparkSession, table: str, since_days: int | None = None) -> DataFrame:
        df = spark.read.parquet(f"{self.lake_path}/bronze/{table}")
        if since_days is not None:  # traitement incrémental : seulement les derniers jours
            df = df.filter(F.col("ingest_date") >= F.date_sub(F.current_date(), since_days))
        return df

    # -- Silver / Gold (Delta) ----------------------------------------------
    def name(self, layer: str, table: str) -> str:
        """Nom lisible dans les logs : table UC ou chemin."""
        return f"{self.schema}.{layer}_{table}" if self.schema else f"{self.lake_path}/{layer}/{table}"

    def exists(self, spark: SparkSession, layer: str, table: str) -> bool:
        if self.schema:
            return spark.catalog.tableExists(self.name(layer, table))
        from delta.tables import DeltaTable
        return DeltaTable.isDeltaTable(spark, self.name(layer, table))

    def read(self, spark: SparkSession, layer: str, table: str) -> DataFrame:
        if self.schema:
            return spark.table(self.name(layer, table))
        return spark.read.format("delta").load(self.name(layer, table))

    def upsert(self, spark: SparkSession, df: DataFrame, layer: str, table: str,
               keys: list[str], partition_by: list[str] | None = None) -> None:
        """MERGE INTO : met à jour les lignes existantes (même clé), ajoute les nouvelles."""
        from delta.tables import DeltaTable

        target = self.name(layer, table)
        if not self.exists(spark, layer, table):
            writer = df.write.format("delta").mode("overwrite")
            if partition_by:
                writer = writer.partitionBy(*partition_by)
            if self.schema:
                writer.saveAsTable(target)
            else:
                writer.save(target)
            log.info("%s créée", target)
            return

        delta = DeltaTable.forName(spark, target) if self.schema else DeltaTable.forPath(spark, target)
        condition = " AND ".join(f"t.{k} = s.{k}" for k in keys)
        (delta.alias("t")
         .merge(df.alias("s"), condition)
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        log.info("%s : MERGE terminé", target)

    def overwrite(self, df: DataFrame, layer: str, table: str, partition_by: list[str] | None = None) -> None:
        """Pour les petites tables d'agrégats recalculées entièrement."""
        writer = df.write.format("delta").mode("overwrite").option("overwriteSchema", "true")
        if partition_by:
            writer = writer.partitionBy(*partition_by)
        target = self.name(layer, table)
        if self.schema:
            writer.saveAsTable(target)
        else:
            writer.save(target)
        log.info("%s réécrite", target)

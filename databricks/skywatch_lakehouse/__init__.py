"""Couches Silver et Gold de SkyWatch (semaine 3), en PySpark + Delta Lake.

Le même code tourne :
  - en local, dans Docker (service `lakehouse`), sur les fichiers de data/lake ;
  - sur Databricks (Free Edition ne supporte pas Scala : tout est en PySpark / SQL).

Les transformations sont des fonctions pures (DataFrame -> DataFrame), testées sans Delta.
"""

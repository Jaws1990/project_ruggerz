# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b932bd1-663b-4c50-9ea1-8789c32b5164",
# META       "default_lakehouse_name": "lakehouse_pr",
# META       "default_lakehouse_workspace_id": "b9149937-7763-4698-89e3-4fb3cc6f069f",
# META       "known_lakehouses": [
# META         {
# META           "id": "5b932bd1-663b-4c50-9ea1-8789c32b5164"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

%run delta_table_manager

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import date
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Key columns to check for changes on (post bronze transformation)
HASH_COLUMNS = ["league_name", "league_type", "logo", "country_id"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_date = date.today()

bronze_df = spark.table("bronze.leagues")

silver_df = (
    bronze_df
    .filter(F.to_date("ingested_at") == target_date)
    .select(
        F.col("id"),
        F.col("name").alias("league_name"),
        F.col("type").alias("league_type"),
        F.col("logo"),
        F.col("country.id").alias("country_id"),
    )
    .withColumn("row_hash", F.sha2(F.concat_ws("||", *[F.col(c) for c in HASH_COLUMNS]), 256))
)

display(silver_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_manager = DeltaTableManager()
table_manager.merge_scd2(silver_df,"silver.leagues",["id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

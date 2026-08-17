# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b932bd1-663b-4c50-9ea1-8789c32b5164",
# META       "default_lakehouse_name": "PR_Lakehouse_01",
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

from datetime import datetime
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_date = "2026-08-10"
#target_date = current_date()

bronze_df = spark.table("bronze.leagues")

silver_df = (
    bronze_df.filter(F.to_date("ingested_at") == target_date)
    .withColumn("league_seasons",F.explode("seasons"))
    .select(
        F.col("id").alias("league_id")
        ,F.col("league_seasons.season").cast("int").alias("season")
        ,F.col("league_seasons.current").alias("is_current")
        ,F.col("league_seasons.start").cast("date").alias("start_date")
        ,F.col("league_seasons.end").cast("date").alias("end_date")
    )
)

display(silver_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_manager = DeltaTableManager()
table_manager.upsert(silver_df,"silver.league_seasons",["league_id","season"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

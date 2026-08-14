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

from datetime import datetime
from pyspark.sql.functions import col,to_date,current_date,explode

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
    bronze_df.filter(to_date("ingested_at") == target_date)
    .withColumn("league_seasons",explode("seasons"))
    .select(
        col("id").alias("league_id")
        ,col("league_seasons.season").cast("int").alias("season")
        ,col("league_seasons.current").alias("is_current")
        ,col("league_seasons.start").cast("date").alias("start_date")
        ,col("league_seasons.end").cast("date").alias("end_date")
    )
)

display(silver_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

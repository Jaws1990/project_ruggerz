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

target_date = "2026-08-11"
#target_date = current_date()

bronze_df = spark.table("bronze.games")

silver_df = (
    bronze_df
    .filter(F.to_date("ingested_at") == target_date)
    .select(
        F.col("id"),
        F.col("country.id").alias("country_id"),
        F.col("league.id").alias("league_id").cast("int"),
        F.col("league.season").alias("season").cast("int"),
        F.col("week").alias("game_week").cast("int"),
        F.col("date").cast("date").alias("kick_off_date"),
        F.col("time").alias("kick_off_time"),
        F.col("timezone"),
        F.col("teams.home.id").alias("home_team_id").cast("int"),
        F.col("teams.away.id").alias("away_team_id").cast("int"),
        F.col("scores.home").alias("home_score").cast("int"),
        F.col("scores.away").alias("away_score").cast("int"),
        F.col("status.long").alias("game_status"),
        F.col("periods.first.home").alias("first_half_home_score").cast("int"),
        F.col("periods.first.away").alias("first_half_away_score").cast("int"),
        F.col("periods.second.home").alias("second_half_home_score").cast("int"),
        F.col("periods.second.away").alias("second_half_away_score").cast("int"),
        F.col("periods.overtime.home").alias("overtime_home_score").cast("int"),
        F.col("periods.overtime.away").alias("overtime_away_score").cast("int"),
        F.col("periods.second_overtime.home").alias("second_overtime_home_score").cast("int"),
        F.col("periods.second_overtime.away").alias("second_overtime_away_score").cast("int"),
    )
    .dropDuplicates()
)

display(silver_df.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_manager = DeltaTableManager()
table_manager.upsert(silver_df,"silver.games",["id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

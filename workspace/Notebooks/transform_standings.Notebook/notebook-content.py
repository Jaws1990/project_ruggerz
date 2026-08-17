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

#standings silver table is a snapshot fact table (standings as per a point in time)

target_date = "2026-08-11"
#target_date = current_date()

bronze_df = spark.table("bronze.standings")

silver_df = (
    bronze_df
    .filter(F.to_date("ingested_at") == target_date)
    .select(
        #making composite key for fields that make a standings record unique
        F.sha2(
            F.concat_ws("||",
                F.col("team.id").cast("string"),
                F.col("league.id").cast("string"),
                F.col("league.season").cast("string"),
                F.coalesce(F.col("stage"), F.lit("")),
                F.coalesce(F.col("group.name"), F.lit("")),
                F.date_format(F.col("ingested_at"), "yyyyMMdd"),
            ), 256,
        ).alias("standings_id"),
        F.col("ingested_at").cast("date").alias("snapshot_date"),
        F.col("country.id").alias("country_id"),
        F.col("league.season").alias("season").cast("int"),
        F.col("league.id").alias("league_id").cast("int"),
        F.col("group.name").alias("group_name"),
        F.col("team.id").alias("team_id"),
        F.col("description"),
        F.col("form"),
        F.col("position").cast("int"),
        F.col("points").cast("int"),
        F.col("goals.for").alias("points_for"),
        F.col("goals.against").alias("points_against"),
        F.col("games.played").alias("games_played"),
        F.col("games.win.total").alias("games_won"),
        F.col("games.lose.total").alias("games_lost"),
        F.col("games.draw.total").alias("games_drawn"),
        F.col("games.win.percentage").cast("decimal(5,2)").alias("won_percentage"),
        F.col("games.lose.percentage").cast("decimal(5,2)").alias("lost_percentage"),
        F.col("games.draw.percentage").cast("decimal(5,2)").alias("drawn_percentage"),
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
table_manager.upsert(silver_df,"silver.standings",["standings_id"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

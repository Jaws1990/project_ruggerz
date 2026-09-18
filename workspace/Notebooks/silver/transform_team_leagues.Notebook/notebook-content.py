# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "a2db8af3-fb31-876c-48fd-b495fd84ee56",
# META       "default_lakehouse_name": "lakehouse_pr",
# META       "default_lakehouse_workspace_id": "00000000-0000-0000-0000-000000000000",
# META       "known_lakehouses": [
# META         {
# META           "id": "a2db8af3-fb31-876c-48fd-b495fd84ee56",
# META           "workspace_id": "00000000-0000-0000-0000-000000000000"
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

target_date = date.today()

bronze_df = spark.table("bronze.teams")

silver_df = (
    bronze_df.filter(F.to_date("ingested_at") == target_date)
    .select(
        F.col("id").alias("team_id"),
        F.col("season"),
        F.col("league_id"),
    ).distinct()
)

display(silver_df.limit(10))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

table_manager = DeltaTableManager()
table_manager.upsert(silver_df,"silver.team_leagues",["team_id","league_id","season"])

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

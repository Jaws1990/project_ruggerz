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
from pyspark.sql.functions import col,to_date,current_date,explode,sha2,concat_ws

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Key columns to check for changes on (post bronze transformation)
HASH_COLUMNS = [
    "team_name"
    "is_national",
    "logo",
    "founded",
    "arena_capacity",
    "arena_location",
    "arena_name",
    "country_id",
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_date = "2026-08-14"
#target_date = current_date()

bronze_df = spark.table("bronze.teams")

silver_df = (
    bronze_df
    .filter(to_date("ingested_at") == target_date)
    .select(
        col("id")
        ,col("name").alias("team_name")
        ,col("national").alias("is_national")
        ,col("logo")
        ,col("founded").cast("int")
        ,col("arena.capacity").cast("int").alias("arena_capacity")
        ,col("arena.location").alias("arena_location")
        ,col("arena.name").alias("arena_name")
        ,col("country.id").cast("int").alias("country_id") 
    )
    .withColumn("row_hash",sha2(concat_ws("||", *[col(c) for c in HASH_COLUMNS]), 256))
    .dropDuplicates()
)

display(silver_df)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

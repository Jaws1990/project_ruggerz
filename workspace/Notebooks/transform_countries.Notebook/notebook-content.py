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
from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

target_date = "2026-08-10"
#target_date = current_date()

bronze_df = spark.table("bronze.countries")

silver_df = (
    bronze_df.filter(F.to_date("ingested_at") == target_date)
    .select(
        "code"
        ,"flag"
        ,"id"
        ,"name"
    )
)
print(silver_df.schema)
display(silver_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("multiline", "true").json(f"{OUTPUT_PATH}/{FILENAME}")
responses = raw_df.withColumn("response", F.explode(F.col("response")))

if not responses.isEmpty():
    bronze_df = responses.select("response.*")

    display(bronze_df.take(5))

    ingestor.write_to_bronze_table(df=bronze_df,table_name=ENTITY,mode="append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

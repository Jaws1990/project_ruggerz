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

%run api_ingestor

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from notebookutils import mssparkutils
from datetime import datetime
from pyspark.sql import functions as F
mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ENDPOINT = "seasons"
ENTITY = "seasons"
DATESTAMP = datetime.now().strftime("%Y%m%d")
FILENAME = f"{ENTITY}.json"
OUTPUT_PATH = f"Files/raw/{ENTITY}/{DATESTAMP}"

ingestor = APIIngestor()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ingestor.download_json(ENDPOINT, OUTPUT_PATH, FILENAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("multiline", "true").json(f"{OUTPUT_PATH}/{FILENAME}")
responses = raw_df.withColumn("response", F.explode(F.col("response")))

if not responses.isEmpty():
    bronze_df = responses.select("season")

    display(bronze_df.take(5))

    ingestor.write_to_bronze_table(df=bronze_df,table_name=ENTITY,mode="overwrite")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

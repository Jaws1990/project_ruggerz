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
mssparkutils.fs.mounts()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from datetime import datetime

ENDPOINT = "https://v1.rugby.api-sports.io/seasons"
ENTITY = "seasons"
DATESTAMP = datetime.now().strftime("%Y%m%d")
FILENAME = f"{ENTITY}.json"
OUTPUT_PATH = f"Files/raw/{ENTITY}/{DATESTAMP}"

ingestor = APIIngestor("7daf8d952d4d4e2575fa88517c912a44")
ingestor.download_json(ENDPOINT, OUTPUT_PATH, FILENAME)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.option("multiline", "true").json("Files/raw/seasons/20260805/seasons.json")
# df now is a Spark DataFrame containing JSON data from "Files/raw/seasons/20260805/seasons.json".
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import explode, col, current_timestamp, input_file_name, regexp_replace

enriched = (
    df.withColumn("season",explode(col("response")))
    .withColumn("source_file", regexp_replace(input_file_name(), r".*?/Files/", "Files/"))
    .withColumn("ingested_at", current_timestamp())
)

display(enriched)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

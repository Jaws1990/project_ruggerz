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

ENDPOINT = "leagues"
ENTITY = "leagues"
DATESTAMP = datetime.now().strftime("%Y%m%d")
FILENAME = f"{ENTITY}.json"
OUTPUT_PATH = f"Files/raw/{ENTITY}/{DATESTAMP}"
ingestor = APIIngestor()

mssparkutils.fs.mounts()

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
    bronze_df = responses.select("response.*")

    display(bronze_df.take(5))

    ingestor.write_to_bronze_table(df=bronze_df,table_name=ENTITY,mode="append")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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
from time import sleep
variable_library = notebookutils.variableLibrary.getLibrary("PR_variables")
mssparkutils.fs.mounts()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

#Get a list of Leagues we want to get data for. 

league_ids = (
    spark.read.format("csv")
    .option("header","true")
    .load("Files/LeagueList.csv")
    .collect()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ENDPOINT = "games"
ENTITY = "games"
DATESTAMP = datetime.now().strftime("%Y%m%d")
FILENAME = f"{ENTITY}.json"
SEASON = variable_library.getVariable("season")
ingestor = APIIngestor()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for league in league_ids:
    league_name = league["name"]
    output_path = f"Files/raw/{ENTITY}/{DATESTAMP}/{league_name}/{SEASON}"
    ingestor.download_json(ENDPOINT, output_path, FILENAME, query_params={"league":league["id"],"season":SEASON})
    #pause so we dont go over request per minute limit
    sleep(7)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("multiline", "true").json(f"Files/raw/{ENTITY}/{DATESTAMP}/*/*/*.json")
responses = raw_df.withColumn("response", F.explode(F.col("response")))

if not responses.isEmpty():
    bronze_df = responses.select("response.*")

    display(bronze_df.take(5))

    ingestor.write_to_bronze_table(
                df=bronze_df,
                table_name=ENTITY,
                mode="merge",
                merge_condition="target.id = source.id")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

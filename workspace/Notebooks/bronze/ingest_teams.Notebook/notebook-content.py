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
from time import sleep
variable_library = notebookutils.variableLibrary.getLibrary("variable_library")
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

ENDPOINT = "teams"
ENTITY = "teams"
DATESTAMP = datetime.now().strftime("%Y%m%d")
FILENAME = f"{ENTITY}.json"
SEASONS = [s.strip() for s in variable_library.getVariable("season").split(",")]
# Teams are assumed to remain the same for every season, so we only load once using the first season in the list.
SEASON = SEASONS[0]
ingestor = APIIngestor()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for league in league_ids:
    league_name = league["name"]
    output_path = f"Files/raw/{ENTITY}/{DATESTAMP}/{league_name}"
    ingestor.download_json(ENDPOINT, output_path, FILENAME, query_params={"league":league["id"],"season":SEASON})
    #pause so we dont go over request per minute limit
    sleep(7)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

raw_df = spark.read.option("multiline", "true").json(f"Files/raw/{ENTITY}/{DATESTAMP}/*/*.json")
responses = raw_df.withColumn("response", F.explode(F.col("response")))

if not responses.isEmpty():
    bronze_df = (
        responses.withColumn("league_id",raw_df.parameters.league)
        .withColumn("season",raw_df.parameters.season)
        .select(["league_id","response.*","season"])
    )

    display(bronze_df.take(5))

    ingestor.write_to_bronze_table(df=bronze_df, table_name=ENTITY, mode="append",
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

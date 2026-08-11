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

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.countries (
# MAGIC     id INT,
# MAGIC     code STRING,
# MAGIC     flag STRING,
# MAGIC     processed_at TIMESTAMP
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.leagues (
# MAGIC     id            BIGINT      NOT NULL,
# MAGIC     league_name   STRING,
# MAGIC     league_type   STRING,
# MAGIC     logo          STRING,
# MAGIC     country_id    BIGINT,
# MAGIC     row_hash      STRING,
# MAGIC     processed_at  TIMESTAMP   NOT NULL,
# MAGIC     valid_from    TIMESTAMP   NOT NULL,
# MAGIC     valid_to      TIMESTAMP,
# MAGIC     is_current    BOOLEAN     NOT NULL
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.league_season (
# MAGIC     league_id     BIGINT      NOT NULL,
# MAGIC     season        INT         NOT NULL,
# MAGIC     is_current    BOOLEAN,
# MAGIC     start_date    DATE,
# MAGIC     end_date      DATE,
# MAGIC     processed_at  TIMESTAMP   NOT NULL
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

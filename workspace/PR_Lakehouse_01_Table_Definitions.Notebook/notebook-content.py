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
# MAGIC CREATE SCHEMA IF NOT EXISTS bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS gold;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
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
# MAGIC CREATE TABLE IF NOT EXISTS silver.teams (
# MAGIC     id              BIGINT      NOT NULL,
# MAGIC     team_name       STRING,
# MAGIC     is_national     BOOLEAN,
# MAGIC     logo            STRING,
# MAGIC     founded         INT,
# MAGIC     arena_capacity  INT,
# MAGIC     arena_location  STRING,
# MAGIC     arena_name      STRING,
# MAGIC     country_id      INT,
# MAGIC     row_hash        STRING      NOT NULL,
# MAGIC     processed_at    TIMESTAMP   NOT NULL,
# MAGIC     valid_from      DATE   NOT NULL,
# MAGIC     valid_to        DATE,
# MAGIC     is_current      BOOLEAN     NOT NULL
# MAGIC )
# MAGIC USING DELTA

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.team_leagues (
# MAGIC     team_id       INT       NOT NULL,
# MAGIC     league_id     INT       NOT NULL,
# MAGIC     season        INT       NOT NULL,
# MAGIC     processed_at  TIMESTAMP NOT NULL
# MAGIC )
# MAGIC USING DELTA

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
# MAGIC     valid_from    DATE   NOT NULL,
# MAGIC     valid_to      DATE,
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
# MAGIC CREATE TABLE IF NOT EXISTS silver.league_seasons (
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

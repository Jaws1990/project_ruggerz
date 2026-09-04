# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "5b932bd1-663b-4c50-9ea1-8789c32b5164",
# META       "default_lakehouse_name": "lakehouse_pr",
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
# MAGIC     id BIGINT,
# MAGIC     country_name STRING,
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
# MAGIC CREATE TABLE IF NOT EXISTS silver.games (
# MAGIC     id                             BIGINT      NOT NULL,
# MAGIC     country_id                     BIGINT,
# MAGIC     league_id                      BIGINT,
# MAGIC     season                         INT,
# MAGIC     game_week                           INT,
# MAGIC     kick_off_date                  DATE,
# MAGIC     kick_off_time                  STRING,
# MAGIC     timezone                       STRING,
# MAGIC     home_team_id                   BIGINT,
# MAGIC     away_team_id                   BIGINT,
# MAGIC     home_score                     INT,
# MAGIC     away_score                     INT,
# MAGIC     game_status                         STRING,
# MAGIC     first_half_home_score          INT,
# MAGIC     first_half_away_score          INT,
# MAGIC     second_half_home_score         INT,
# MAGIC     second_half_away_score         INT,
# MAGIC     overtime_home_score            INT,
# MAGIC     overtime_away_score            INT,
# MAGIC     second_overtime_home_score     INT,
# MAGIC     second_overtime_away_score     INT,
# MAGIC     processed_at                   TIMESTAMP   NOT NULL
# MAGIC )
# MAGIC USING DELTA;

# METADATA ********************

# META {
# META   "language": "sparksql",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# MAGIC %%sql
# MAGIC CREATE TABLE IF NOT EXISTS silver.standings (
# MAGIC     standings_id       STRING NOT NULL,
# MAGIC     snapshot_date      DATE  NOT NULL,
# MAGIC     country_id         BIGINT,
# MAGIC     season             INT,
# MAGIC     league_id          BIGINT,
# MAGIC     group_name         STRING,
# MAGIC     team_id            BIGINT,
# MAGIC     description        STRING,
# MAGIC     form               STRING,
# MAGIC     position           INT,
# MAGIC     points             INT,
# MAGIC     points_for         INT,
# MAGIC     points_against     INT,
# MAGIC     games_played       INT,
# MAGIC     games_won          INT,
# MAGIC     games_lost         INT,
# MAGIC     games_drawn        INT,
# MAGIC     won_percentage     DECIMAL(5,2),
# MAGIC     lost_percentage    DECIMAL(5,2),
# MAGIC     drawn_percentage   DECIMAL(5,2),
# MAGIC     processed_at       TIMESTAMP   NOT NULL
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
# MAGIC     country_id      BIGINT,
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
# MAGIC     team_id       BIGINT       NOT NULL,
# MAGIC     league_id     BIGINT       NOT NULL,
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

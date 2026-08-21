# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A research/learning project building a Microsoft Fabric medallion pipeline for Rugby data:

```
Rugby API (api-sports.io) → Bronze (raw Delta) → Silver (clean, historised) → Gold (dbt star schema) → Power BI
```

Design choices are made to be explained and justified for learning purposes, not to be "best practice" by default — see README.md for the reasoning (e.g. why dbt is used narrowly in Gold rather than owning history via snapshots).

This is a Fabric Git-integrated workspace: everything under `workspace/` is a Fabric item (Notebook, Lakehouse, VariableLibrary, DataBuildToolJob) serialized to source-controlled files, not a conventional application repo. There is no local build/lint/test tooling — notebooks and dbt models are run inside the Fabric workspace itself.

## Repo structure

- `workspace/Notebooks/*.Notebook/notebook-content.py` — PySpark notebooks, one per Bronze ingestion (`ingest_*`) and one per Silver transformation (`transform_*`), plus two shared utility notebooks:
  - `api_ingestor.Notebook` — `APIIngestor` class: calls the rugby API, saves raw JSON to Files, and writes Bronze Delta tables (`write_to_bronze_table`, supporting overwrite/append/merge).
  - `delta_table_manager.Notebook` — `DeltaTableManager` class: `upsert()` for plain merge-upsert tables, `merge_scd2()` for SCD Type 2 merges (expires changed rows via `is_current`/`valid_to`, inserts new versions via `row_hash` comparison).
- `workspace/PR_Lakehouse_01_Table_Definitions.Notebook/notebook-content.py` — the **single source-controlled DDL notebook**. All `bronze`/`silver`/`gold` schema and table creation (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`) lives here and must be run in each environment (Dev/Test/Prod) to keep Lakehouse structure reproducible — Fabric Git/Deployment Pipelines do not deploy Lakehouse or table schemas themselves.
- `workspace/PR_Lakehouse_01.Lakehouse/` — Lakehouse item metadata (no data, just Fabric item definition/shortcuts).
- `workspace/PR_variables.VariableLibrary/variables.json` — Fabric variable library (e.g. `season`, `api_base_url`, `key_vault_url`); referenced in notebooks via `notebookutils.variableLibrary.getLibrary("PR_variables")`.
- `workspace/dbtJob_1.DataBuildToolJob/Code/dbt/` — the dbt-fabric project for the Gold layer (`dbt_project.yml`, `models/`). Currently minimal (`FactGames.sql` selects from `silver.games` via `sources.yml`); Gold dimension/fact models with surrogate keys are still to-do (see README's To-do list).
- `workspace/sandbox.Notebook/` — scratch notebook, not part of the pipeline.

## Fabric notebook file format

Notebook files (`notebook-content.py`) are plain `.py` files using Fabric's cell-marker format, not `.ipynb`:

```
# CELL ********************
<code>
# METADATA ********************
# META { "language": "python"|"sparksql", "language_group": "synapse_pyspark" }
```

SQL cells are written as PySpark `%%sql` magic cells (`# MAGIC %%sql` prefix on each line). When editing a notebook, preserve this marker structure exactly — Fabric's Git integration round-trips on it.

## Medallion patterns (see README.md for full entity table)

- **Bronze**: minimal transformation, raw capture. Append or merge depending on entity (e.g. `games` uses merge on `game_id`; most others append).
- **Silver**: one of three explicit patterns per entity, not applied uniformly:
  - SCD1 (`countries`, `league_seasons`) — upsert, no history.
  - SCD2 (`leagues`, `teams`) — full history via `merge_scd2`, using a `row_hash` column to detect changes and `is_current`/`valid_from`/`valid_to` to track validity.
  - Periodic snapshot (`standings`) — every pull is a new row, not merged into history.
  - Bridge table (`team_leagues`) — upsert, no history.
  - `games` is a fact table (merge on `game_id`).
- **Gold**: dbt builds the star schema and surrogate keys on top of already-historicised Silver tables. dbt snapshots are deliberately not used, since Silver already owns change-tracking. Surrogate keys use the `SID` naming convention (e.g. `competition_season_SID`), generated via `xxhash64` over the natural key columns when natural keys are not suitable.

Orchestration is via Fabric Data Pipelines triggering notebooks — nothing is intended to run ad hoc in production.

# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project overview

A research/learning project building a Microsoft Fabric medallion pipeline for Rugby data:

```
Rugby API (api-sports.io) → Bronze (raw Delta) → Silver (clean, historised) → Gold (dbt star schema) → Power BI
```

Design choices are made to be explained and justified for learning purposes, not to be "best practice" by default — see README.md for the reasoning (e.g. why dbt is used narrowly in Gold rather than owning history via snapshots).

This is a Fabric Git-integrated workspace: everything under `workspace/` is a Fabric item (Notebook, Lakehouse, VariableLibrary, DataBuildToolJob, SemanticModel, Report, DataPipeline) serialized to source-controlled files, not a conventional application repo. There is no local build/lint/test tooling — notebooks and dbt models are run inside the Fabric workspace itself.

## Repo structure

- `workspace/notebooks/bronze/*.Notebook/notebook-content.py` — one PySpark notebook per Bronze ingestion (`ingest_*`).
- `workspace/notebooks/silver/*.Notebook/notebook-content.py` — one PySpark notebook per Silver transformation (`transform_*`).
- `workspace/notebooks/utilities/` — shared/support notebooks, not part of the Bronze/Silver layer split:
  - `api_ingestor.Notebook` — `APIIngestor` class: calls the rugby API, saves raw JSON to Files, and writes Bronze Delta tables (`write_to_bronze_table`, supporting overwrite/append/merge).
  - `delta_table_manager.Notebook` — `DeltaTableManager` class: `upsert()` for plain merge-upsert tables, `merge_scd2()` for SCD Type 2 merges (expires changed rows via `is_current`/`valid_to`, inserts new versions via `row_hash` comparison).
  - `lakehouse_pr_table_definitions.Notebook/notebook-content.py` — the **single source-controlled DDL notebook**. All `bronze`/`silver`/`gold` schema and table creation (`CREATE SCHEMA IF NOT EXISTS`, `CREATE TABLE IF NOT EXISTS`) lives here and must be run in each environment (Dev/Test/Prod) to keep Lakehouse structure reproducible — Fabric Git/Deployment Pipelines do not deploy Lakehouse or table schemas themselves.
  - `sandbox.Notebook/` — scratch notebook, not part of the pipeline.
- `workspace/lakehouse_pr.Lakehouse/` — Lakehouse item metadata (no data, just Fabric item definition/shortcuts).
- `workspace/variable_library.VariableLibrary/variables.json` — Fabric variable library (e.g. `season`, `api_base_url`, `key_vault_url`); referenced in notebooks via `notebookutils.variableLibrary.getLibrary("variable_library")`.
- `workspace/dbt_gold_layer.DataBuildToolJob/Code/dbt/` — the dbt-fabric project for the Gold layer (`dbt_project.yml`, `models/`). Currently minimal (`FactGames.sql` selects from `silver.games` via `sources.yml`); Gold dimension/fact models with surrogate keys are still to-do (see README's To-do list).

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
- **Gold**: dbt builds the star schema and surrogate keys on top of already-historicised Silver tables. dbt snapshots are deliberately not used, since Silver already owns change-tracking. Surrogate keys use the `_key_` naming convention (e.g. `competition_season_key`), generated via `xxhash64` over the natural key columns when natural keys are not suitable.

Orchestration is via Fabric Data Pipelines triggering notebooks — nothing is intended to run ad hoc in production.

## Active work in progress (as of 2026-08-28)

**Goal:** build a Power BI report ("The Offload") on top of the Gold star schema, via the `powerbi-authoring` skill family (`powerbi-report-design` → `semantic-model-authoring` → `powerbi-report-authoring`).

**Semantic model — measures work is COMPLETE, plus a fact_games schema change.** `workspace/semantic_model_the_offload.SemanticModel` (Direct Lake, TMDL, 8 tables: `fact_games`, `fact_standings`, `dim_teams`, `dim_competitions`, `dim_competition_seasons`, `dim_game_dates`, `dim_snapshot_dates`, `bridge_game_team`). Relationships are correctly wired (including `fact_standings.team_key -> dim_teams.team_key` and the `bridge_game_team` many-to-many for `fact_games` ↔ `dim_teams`).

All 4 measure batches from `C:\Users\luke_\.claude\plans\ok-the-model-tables-woolly-summit.md` are on disk (that file has the full rationale/history if needed, but is not required reading to continue — the summary below is sufficient):
- `fact_games`: `Count of Games`, `Latest Kick Off Date` (folder "Freshness"); `Points Scored`, `Average Points per Game`, `Average Winning Margin`, `Home Wins`, `Away Wins`, `Draws`, `Home Win %`, `Away Win %`, `Draw %` (folder "Scoring").
- `fact_standings`: `Latest Snapshot Date`, `Current Position`, `Current Points`, `Current Games Played`, `Current Win %`, `Current Draw %`, `Current Loss %` (folder "Standings - Current"); `Weekly Comparison Date`, `Monthly Comparison Date`, `Previous Snapshot Date (Weekly/Monthly)`, `Previous Position (Weekly/Monthly)`, `Position Change (Weekly/Monthly)`, `League Volatility (Weekly/Monthly)` (folder "Standings - Movement"). Also fixed: `won_percentage`/`lost_percentage`/`drawn_percentage` were wrongly formatted as GBP currency, now `0%`.
- Movement measures pin to `competition_season_key` via `SELECTEDVALUE` so a season boundary or cup-vs-league mix can never be compared as if it were form (fails safe to blank if context is ambiguous).



**Mechanics note for future model edits:** the `powerbi-modeling-mcp` MCP tool works against this folder via `connection_operations` `ConnectFolder` (path: `workspace/semantic_model_the_offload.SemanticModel`), but it only edits an **in-memory** copy — changes must be exported (`measure_operations`/`table_operations` `ExportTMDL`) and hand-written back into the `.tmdl` files on disk to actually persist, since there's no live Fabric/Desktop connection in this environment. A fresh chat means a fresh MCP session with no memory of prior in-memory edits — always verify what's actually on disk before assuming an object exists.


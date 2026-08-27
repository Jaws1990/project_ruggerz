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
- **Gold**: dbt builds the star schema and surrogate keys on top of already-historicised Silver tables. dbt snapshots are deliberately not used, since Silver already owns change-tracking. Surrogate keys use the `_key_` naming convention (e.g. `competition_season_key`), generated via `xxhash64` over the natural key columns when natural keys are not suitable.

Orchestration is via Fabric Data Pipelines triggering notebooks — nothing is intended to run ad hoc in production.

## Active work in progress (as of 2026-08-27)

**Goal:** build a Power BI report ("The Offload") on top of the Gold star schema, via the `powerbi-authoring` skill family (`powerbi-report-design` → `semantic-model-authoring` → `powerbi-report-authoring`).

**Semantic model:** `workspace/the_offload_semantic_model.SemanticModel` (Direct Lake, TMDL, 8 tables: `fact_games`, `fact_standings`, `dim_teams`, `dim_competitions`, `dim_competition_seasons`, `dim_game_dates`, `dim_snapshot_dates`, `bridge_game_team`). Relationships are now correctly wired (including `fact_standings.team_key -> dim_teams.team_key` and the `bridge_game_team` many-to-many for `fact_games` ↔ `dim_teams`). `fact_games.home_team_key`/`away_team_key` remain unwired orphan columns — not currently blocking anything, since no measure built so far needs home/away team identity directly on `fact_games`.

**Measures work — currently paused mid-batch.** Full roadmap (4 batches, naming conventions, rationale) is in the plan file `C:\Users\luke_\.claude\plans\ok-the-model-tables-woolly-summit.md` — **read this first** when resuming. Status:
- Batch 1 (`displayFolder: "Freshness"`): `Count of Games` and `Latest Kick Off Date` are written to `workspace/the_offload_semantic_model.SemanticModel/definition/tables/fact_games.tmdl` (done, on disk). `Count of Standings Snapshots` was created in an MCP in-memory session but **not yet written to `fact_standings.tmdl`** — the file on disk still has zero measures. Do this first when resuming: add
  ```
  /// Number of standings snapshot rows in current filter context.
  measure 'Count of Standings Snapshots' = COUNTROWS(fact_standings)
  	formatString: #,0
  	displayFolder: Freshness
  	lineageTag: 17c74239-b492-4910-85af-589518ff2c80
  ```
  right after the `table fact_standings` header block (before `column standings_key`).
- Batches 2–4 (game scoring %s, current-snapshot standings measures, rank-movement/volatility measures) not started. Batch 3 also includes fixing `fact_standings.won_percentage`/`lost_percentage`/`drawn_percentage`, which are still wrongly formatted as GBP currency instead of percentage.

**Mechanics note:** the `powerbi-modeling-mcp` MCP tool works against this folder via `connection_operations` `ConnectFolder` (path: `workspace/the_offload_semantic_model.SemanticModel`), but it only edits an **in-memory** copy — changes must be exported (`measure_operations`/`table_operations` `ExportTMDL`) and hand-written back into the `.tmdl` files on disk to actually persist, since there's no live Fabric/Desktop connection in this environment. A fresh chat means a fresh MCP session with no memory of measures already created in-memory in a prior session — always verify what's actually on disk before assuming a measure exists.

**Report design (not yet persisted to a file):** a full `Design Brief:` YAML contract (tone: "Sports Broadcast", signature: status-coded W/L/D coloring, 2 pages — "Standings & Form" and "Fixtures & Results" — with full layout_contract geometry) was produced collaboratively in chat but **only exists in that conversation's history**, not saved anywhere in the repo. If resuming report design/authoring work and that conversation isn't available, the brief will need to be re-derived or re-discussed with the user rather than assumed to exist. No PBIP/Report files exist yet — `powerbi-report-authoring` hasn't been invoked.

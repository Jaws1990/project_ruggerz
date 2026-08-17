# project_ruggerz

**This is a research and learning project.** It exists to build skills in Microsoft Fabric data engineering. Design choices below are made to be explained and justified, not to look impressive. The project's analytical goal is to produce comprehensive and easy to use data for Rugby results. 

## Goal

Build a medallion (Bronze/Silver/Gold) pipeline from a sports API through to a Power BI report, using:

```
Rugby API → Bronze (raw Delta) → Silver (clean, historised) → Gold (dbt star schema) → Power BI
```

## Architecture

| Layer | Purpose | Tooling |
|---|---|---|
| **Bronze** | Capture raw, decide structure later. Minimal transformation. | PySpark notebooks, Delta Lake |
| **Silver** | Explicit, typed, deduplicated, historised entities | PySpark notebooks, Delta Lake |
| **Gold** | Star schema with surrogate keys | dbt-fabric |
| **Serving** | Semantic model + report | Power BI |

Orchestration: Fabric Data Pipelines trigger every notebook — nothing runs ad hoc in production.

Silver table schemas (`CREATE TABLE IF NOT EXISTS` DDL) live in a single version-controlled schemas notebook — see `/notebooks/silver_schemas`.

## Entities

| Entity | Ingestion schedule | Raw folder structure | Bronze write | Silver entity | Silver pattern |
|---|---|---|---|---|---|
| Countries | Adhoc | `raw/countries/{load_date}` | Append | `silver.countries` | SCD1 |
| League | Adhoc | `raw/seasons/{load_date}` | Append | `silver.leagues` | SCD2 |
| League | Adhoc | `raw/seasons/{load_date}` | Append | `silver.league_seasons` | SCD1 |
| Teams | Adhoc | `raw/teams/{load_date}/{league}` | Append | `silver.teams` | SCD2 |
| Teams | Adhoc | `raw/teams/{load_date}/{league}` | Append | `silver.team_leagues` (bridge) | Upsert, no history |
| Standings | Weekly | `raw/standings/{load_date}/{league}/{season}` | Append | `silver.standings` | Periodic snapshot |
| Games | Weekly | `raw/games/{load_date}/{league}/{season}` | Merge (on `game_id`) | `silver.games` | Fact (not yet designed) |

**Why three different Silver patterns?** Kimball distinguishes dimension history (SCD2) from fact history (periodic snapshot). A team renaming is a dimension change worth versioning; a weekly standings pull is a fact measured repeatedly — forcing SCD2 onto it would be modelling the wrong thing.

## Why dbt for Gold?

A deliberate learning choice, not a "best tool for the job" call — this project is also about building dbt fluency for the CV. Silver already performs its own change-tracking (hand-rolled PySpark MERGE for SCD2), so dbt's job in Gold is narrower than textbook: it adds surrogate keys and builds the star schema on top of already-historicised Silver tables, rather than owning history itself. dbt snapshots are intentionally not used here for that reason.

## To-do

- [x] Source/API research and entity hierarchy mapping
- [x] Medallion architecture and entity classification (slow vs fast-changing)
- [x] Bronze ingestion design (raw capture, append vs merge per entity)
- [x] Bronze ingestion notebooks (countries, leagues, teams, standings, games)
- [x] Silver design: SCD1 / SCD2 / periodic snapshot / bridge patterns
- [x] Silver schemas notebook (DDL for tables built so far)
- [x] Silver transform: `leagues` (SCD2), `league_seasons` (SCD1)
- [x] Silver transform: `teams` (SCD2), `team_leagues` (bridge)
- [x] Silver transform: `countries` (SCD1)
- [x] Silver transform: `standings` (periodic snapshot)
- [x] Silver transform: `games` (fact table)
- [x] Shared utility module (`scd2_merge`, `upsert`)
- [ ] Fabric Data Pipelines — orchestration and scheduling for all notebooks
- [x] Fabric variable library — season parameter
- [x] Lakehouse CSV — league scope config
- [ ] dbt-fabric project setup, connected to Silver
- [ ] Gold: dimension models with surrogate keys
- [ ] Gold: fact models (games, standings)
- [ ] Power BI semantic model
- [ ] Power BI report

## Repo structure

```
/workspace
    /notebooks  
        - one notebook per Bronze entity ingestion
        - one notebook per Silver table transformation
    fabric items
readme.md
```

## Lakehouse Schema Management

Fabric Git and Deployment Pipelines do not currently deploy Lakehouse schemas or table schemas. Schema management is therefore handled through the **Delta Table Definitions notebook**, which is source controlled and run in each environment.

### Approach

- Create schemas using `CREATE SCHEMA IF NOT EXISTS`.
- Create new tables using `CREATE TABLE IF NOT EXISTS`.
- Manage changes to existing tables through explicit schema migrations.
- Use `ADD COLUMNS IF NOT EXISTS` for non-destructive column additions.
- Handle renames and removals through explicit `ALTER TABLE` statements.
- Do not automatically drop columns or tables.
- Keep all schema changes in Git so Dev → Test → Prod remains reproducible.

The Delta Table Definitions notebook is therefore the **source-controlled mechanism for managing Lakehouse structure across environments**.

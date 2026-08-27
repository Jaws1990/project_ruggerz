-- bridge_game_team.sql
-- Unpivots home_team_key/away_team_key from fact_games so a team slices
-- across all its games, home or away.
--
-- Full rebuild every run (not incremental) - scans all of fact_games each
-- time. Fine at this scale. If this ever needs to scale up: incremental,
-- unique_key=['game_key','team_key'], insert-only - home/away doesn't
-- change once a game exists, unlike scores/status (why fact_games uses merge).

{{ config(materialized='table') }}

with home as (

    select
        game_key,
        home_team_key as team_key,
        'Home' as team_role
    from {{ ref('fact_games') }}

),

away as (

    select
        game_key,
        away_team_key as team_key,
        'Away' as team_role
    from {{ ref('fact_games') }}

)

select * from home
union all
select * from away
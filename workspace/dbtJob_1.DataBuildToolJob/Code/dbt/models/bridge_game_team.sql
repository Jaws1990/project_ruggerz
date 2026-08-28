-- bridge_game_team.sql
-- Unpivots home/away team_key so a team slices across all its games, home
-- or away. fact_games no longer carries home_team_key/away_team_key (it
-- resolves home_team_name/away_team_name instead), so this model re-derives
-- team_key independently via the same temporal join fact_games uses against
-- dim_teams.
--
-- Full rebuild every run (not incremental) - scans all of silver.games each
-- time. Fine at this scale. If this ever needs to scale up: incremental,
-- unique_key=['game_key','team_key'], insert-only - home/away doesn't
-- change once a game exists, unlike scores/status (why fact_games uses merge).

{{ config(materialized='table') }}

with home as (

    select
        f.id as game_key,
        t_home.team_key as team_key,
        'Home' as team_role
    from {{ source('silver', 'games') }} f
    left join {{ ref('dim_teams') }} t_home
        on f.home_team_id = t_home.team_id
       and f.kick_off_date >= t_home.valid_from
       and f.kick_off_date <  COALESCE(t_home.valid_to, '2099-01-01')

),

away as (

    select
        f.id as game_key,
        t_away.team_key as team_key,
        'Away' as team_role
    from {{ source('silver', 'games') }} f
    left join {{ ref('dim_teams') }} t_away
        on f.away_team_id = t_away.team_id
       and f.kick_off_date >= t_away.valid_from
       and f.kick_off_date <  COALESCE(t_away.valid_to, '2099-01-01')

)

select * from home
union all
select * from away
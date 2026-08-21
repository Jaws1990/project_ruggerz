{{
    config(
        materialized='incremental',
        unique_key='id',
        incremental_strategy='merge'
    )
}}

select
    f.id as game_key,
    f.kick_off_date,
    f.kick_off_time,
    f.game_week,
    f.home_score,
    f.away_score,
    f.game_status,
    f.first_half_home_score,
    f.first_half_away_score,
    f.second_half_home_score,
    f.second_half_away_score,
    f.overtime_home_score,
    f.overtime_away_score,
    f.second_overtime_home_score,
    f.second_overtime_away_score,
    t_home.team_key                          as home_team_key,
    t_away.team_key                          as away_team_key,
    comp_s.competition_season_key,
    comp.competition_key
from  {{ source('silver', 'games') }} f
left join {{ ref('dim_teams') }} t_home
    on f.home_team_id = t_home.team_id
   and f.kick_off_date >= t_home.valid_from
   and f.kick_off_date <  t_home.valid_to
left join {{ ref('dim_teams') }} t_away
    on f.away_team_id = t_away.team_id
   and f.kick_off_date >= t_away.valid_from
   and f.kick_off_date <  t_away.valid_to
left join {{ ref('dim_competitions') }} comp
    on f.league_id = comp.competition_id
   and f.kick_off_date >= comp.valid_from
   and f.kick_off_date <  comp.valid_to
left join {{ ref('dim_competition_seasons') }} comp_s
    on f.league_id = comp_s.league_id
   and f.season    = comp_s.season

{% if is_incremental() %}
    where f.processed_at > (select max(processed_at) from {{ this }})
{% endif %} 
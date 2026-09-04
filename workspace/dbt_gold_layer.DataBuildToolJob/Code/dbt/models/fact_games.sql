{{
    config(
        materialized='incremental',
        unique_key='game_key',
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
    t_home.team_name                         as home_team_name,
    t_away.team_name                         as away_team_name,
    abs(f.home_score - f.away_score)         as margin,
    case
        when f.home_score is null or f.away_score is null then null
        when f.home_score > f.away_score then 'Home Win'
        when f.away_score > f.home_score then 'Away Win'
        else 'Draw'
    end                                       as result,
    comp_s.competition_season_key,
    comp.competition_key,
    CURRENT_TIMESTAMP AS loaded_at
from  {{ source('silver', 'games') }} f
left join {{ ref('dim_teams') }} t_home
    on f.home_team_id = t_home.team_id
   and f.kick_off_date >= t_home.valid_from
   and f.kick_off_date <  COALESCE(t_home.valid_to, '2099-01-01')
left join {{ ref('dim_teams') }} t_away
    on f.away_team_id = t_away.team_id
   and f.kick_off_date >= t_away.valid_from
   and f.kick_off_date <  COALESCE(t_away.valid_to, '2099-01-01')
left join {{ ref('dim_competitions') }} comp
    on f.league_id = comp.competition_id
   and f.kick_off_date >= comp.valid_from
   and f.kick_off_date <  COALESCE(comp.valid_to, '2099-01-01')
left join {{ ref('dim_competition_seasons') }} comp_s
    on f.league_id = comp_s.competition_id
   and f.season    = comp_s.season

{% if is_incremental() %}
    where f.processed_at > (select COALESCE(max(loaded_at), '1900-01-01') from {{ this }})
{% endif %} 
{{
    config(
        materialized='incremental',
        unique_key='standings_key',
        incremental_strategy='merge'
    )
}}

select
    abs(xxhash64(f.standings_id))             as standings_key,
    f.snapshot_date,
    f.position,
    f.points,
    f.points_for,
    f.points_against,
    f.games_played,
    f.games_won,
    f.games_lost,
    f.games_drawn,
    f.won_percentage,
    f.lost_percentage,
    f.drawn_percentage,
    f.form,
    f.description,
    f.group_name,
    t.team_key,
    comp_s.competition_season_key,
    comp.competition_key,
    CURRENT_TIMESTAMP AS loaded_at
from {{ source('silver', 'standings') }} f
left join {{ ref('dim_teams') }} t
    on f.team_id = t.team_id
   and f.snapshot_date >= t.valid_from
   and f.snapshot_date <  COALESCE(t.valid_to, '2099-01-01')
left join {{ ref('dim_competitions') }} comp
    on f.league_id = comp.competition_id
   and f.snapshot_date >= comp.valid_from
   and f.snapshot_date <  COALESCE(comp.valid_to, '2099-01-01')
left join {{ ref('dim_competition_seasons') }} comp_s
    on f.league_id = comp_s.competition_id
   and f.season    = comp_s.season

{% if is_incremental() %}
    where f.processed_at > (select COALESCE(max(loaded_at), '1900-01-01') from {{ this }})
{% endif %}

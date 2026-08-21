select
    abs(xxhash64(season, league_id)) as competition_season_SID,
    league_id,
    season,
    is_current,
    start_date,
    end_date
from {{ source('silver', 'league_seasons') }}

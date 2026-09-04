select
    abs(xxhash64(t.id, valid_from, row_hash)) as team_key,
    t.id as team_id,
    team_name,
    is_national,
    logo,
    founded,
    c.country_name,
    c.flag as country_flag,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'teams') }} t
left join {{ source('silver', 'countries') }} c
    on t.country_id = c.id
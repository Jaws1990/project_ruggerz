select
    xxhash64(team_id, valid_from, row_hash) as team_key,
    id as team_id,
    team_name,
    is_national,
    logo,
    founded
    country_name,
    country_flag,
    valid_from,
    valid_to,
    is_current
from {{ source('silver', 'teams') }} t
left join {{ source ('silver', 'countries')}} c
on {{ source('silver', 'teams') }}.country_id = {{ source ('silver', 'countries')}}.country_id
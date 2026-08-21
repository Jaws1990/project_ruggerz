SELECT 
abs(xxhash64(id, row_hash, valid_from)) as competition_key,
id as competition_id,
league_name as competition_name,
league_type as competition_type,
logo,
valid_from,
valid_to,
is_current
FROM
{{ source('silver', 'leagues') }}
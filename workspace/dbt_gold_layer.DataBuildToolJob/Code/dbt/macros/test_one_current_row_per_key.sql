{% test one_current_row_per_key(model, natural_key_columns) %}

with current_rows as (
    select {{ natural_key_columns | join(', ') }}
    from {{ model }}
    where is_current = true
)

select {{ natural_key_columns | join(', ') }}
from current_rows
group by {{ natural_key_columns | join(', ') }}
having count(*) > 1

{% endtest %}

with final as(
    select *
    from {{ref('stg_ship_method')}}
)
select *
from final
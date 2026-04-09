with cte as(
    select
        product_id,
        list_price as price,
        start_date,
        lead(start_date) over (partition by product_id order by start_date) as next_start_date
    from {{ref('stg_product_list_price_history')}}
)
select
    product_id,
    price,
    start_date,
    case
        when next_start_date is not null then DATE_SUB(next_start_date, INTERVAL 1 DAY)
        else NULL
    end as end_date,
    case
        when next_start_date is null then True
        else FALSE
    end as is_current
from cte
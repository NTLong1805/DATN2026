with final as(
    select *
    from {{ref('stg_transaction_history')}}

    union distinct

    select *
    from {{ref('stg_transaction_history_archive')}}
)
select
    _id,
    product_id,
    order_id,
    order_number,
    order_date,
    coalesce(sum(case when transaction_type = 'P' then quantity end),0) as purchase_quantity,
    coalesce(SUM(case when transaction_type = 'S' then quantity end),0) as sell_quantity,
    coalesce(Sum(case when transaction_type = 'W' then quantity end),0) as work_quantity,
    coalesce(sum(case when transaction_type = 'P' then cost_amount end),0) as purchase_cost,
    coalesce(sum(case when transaction_type = 'S' then cost_amount end),0) as sell_cost,
    coalesce(sum(case when transaction_type = 'W' then cost_amount end),0) as work_cost
from final
group by _id,product_id,order_id,order_number,order_date
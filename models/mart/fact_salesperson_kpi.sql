with sales_performance as (
    select
        sp.salesperson_id,
        sp.territory_id,
        sp.territory_name,
        sp.country_region_code,
        sum()
    from {{ref('fact_salesperson_quota')}} sp
    left join {{ref('dim_order')}} dod
    on sp.salesperson_id = dod.salesperson_id
    left join {{ref('fact_order')}} fod
    on fod._id = dod.order_id
    where dod.order_date
)
with sales_performance as (
    select
        sp.salesperson_id,
        sp.territory_id,
        sp.territory_name,
        sp.country_region_code,
        sp.start_date,
        sp.end_date,
        sp.sales_quota_history,
        sp.bonus_amount,
        sp.commission_amount as commission_pct,
        sum(fod.line_total) as total_sales,
        count(distinct dod.order_id) as total_order_sales,
        count(distinct dod.customer_id) as total_customer_serve,
        sum(fod.line_total) / count(distinct dod.order_id) as average_value_order,
        ROUND(sum(fod.line_total) / sp.sales_quota_history * 100,2)  as KPI,
        round(sp.commission_amount * sum(fod.line_total),2) as commission_amount,
        rank() over(partition by sp.start_date order by sum(fod.line_total) desc) as rank_per_quota
    from {{ref('fact_salesperson_quota')}} sp
    left join {{ref('dim_order')}} dod
    on sp.salesperson_id = dod.salesperson_id and dod.order_date between sp.start_date and sp.end_date
    left join {{ref('fact_order')}} fod
    on fod._id = dod.order_id
    group by 1,2,3,4,5,6,7,8,9
)
select *
from sales_performance
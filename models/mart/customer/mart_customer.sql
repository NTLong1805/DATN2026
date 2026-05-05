with metrics as(
    select
        customer_id,
        round(sum(line_total),2) as revenue,
        round(sum(line_total) - sum(order_quantity * standard_cost),2) as profit,
        round((sum(line_total) - sum(order_quantity * standard_cost)) / sum(line_total),2) as profit_margin,
        count(distinct order_id) as total_order,
        count(
            distinct
                case
                    when is_online = '1' then order_id end
        ) as online_order,
        count(
            distinct
                case
                    when is_online = '0' then order_id end
        ) as offline_order,
        round(sum(line_total) / count(distinct order_id),2) as avg_per_order,
    from {{ref('core_order_execution')}}
    group by customer_id
),
final as(
    select c.*,m.*
    from {{ref('dim_customer')}} as c
    left join metrics m
        on m.customer_id = c._id
)
select *
from final
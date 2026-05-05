with rev_per_product as(
    select
        subcategory_name,
        sum(line_total) as revenue
    from {{ref('fact_order')}}
    group by 1
),
pareto as(
    select
        subcategory_name,
        revenue,
        revenue / sum(revenue) over () as pct_contribution,
        sum(revenue) over (order by revenue desc) / sum(revenue) over() as cumulative_pct
    from rev_per_product
),
final as(
    select
        subcategory_name,
        round(revenue,2) as revenue,
        round(pct_contribution,2) as pct_contribution,
        round(cumulative_pct,2) as cumulative_pct,
        case
            when cumulative_pct < 0.8 then 'Top 80% Revenue'
            else 'Remaining 20%'
        end as pareto_flag
    from pareto
)
select *
from final
order by revenue desc
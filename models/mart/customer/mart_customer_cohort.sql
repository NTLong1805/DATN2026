with base as(
    select
        customer_id,
        date_trunc(order_date,Month) as month
    from {{ref('core_order_execution')}}
    group by 1,2
),
first_month as(
    select
        customer_id,
        min(month) as first_month_purchase
    from base
    group by 1
),
activity_month as(
    select
        b.customer_id,
        f.first_month_purchase,
        b.month as buy_month
    from base b
    left join first_month as f
        on f.customer_id = b.customer_id
),
final as(
    select
        customer_id,
        first_month_purchase,
        buy_month,
        date_diff(date(buy_month),date(first_month_purchase),month) as month_diff_from_first
    from activity_month
),
cohort_size as(
    select
        first_month_purchase,
        count(distinct customer_id) as cohort_size
    from first_month
    group by 1
)
select
    f.first_month_purchase,
    f.month_diff_from_first,
    count(distinct f.customer_id) as customer_cnt,
    cs.cohort_size,
    count(distinct f.customer_id) / cs.cohort_size as retention_rate
from final f
left join cohort_size cs
on f.first_month_purchase = cs.first_month_purchase
group by 1,2,4
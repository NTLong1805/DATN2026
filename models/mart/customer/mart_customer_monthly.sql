with base as(
    select distinct
        customer_id,
        date_trunc(order_date,Month) as month
    from {{ref('core_order_execution')}}
),
first_purchase as(
    select
        customer_id,
        min(month) as first_date_purchase
    from base
    group by 1
),
behavior as(
    select
        customer_id,
        month,
        lead(month) over(partition by customer_id order by month) as next_month,
        lag(month) over(partition by customer_id order by month) as last_month
    from base
),
customer_flag as(
    select
        b.customer_id,
        b.month,

        case
            when b.month = fp.first_date_purchase then 1 else 0
        end as is_new,

        case
            when date_diff(date(b.month),date(b.last_month),Month) = 1 then 1 else 0
        end as is_consecutive,

        case
            when next_month is null and date_diff(date(b.month),date(b.last_month),day) > 120
            then 1 else 0
        end as is_churn
    from behavior b
    left join first_purchase fp
    on b.customer_id = fp.customer_id
),
agg as(
    select
        month,
        count(distinct customer_id) as active_customer,
        count(distinct case when is_new = 1 then customer_id end) as new_customer,
        count(distinct customer_id) - count(distinct case when is_new = 1 then customer_id end) as return_customer,
        count(distinct case when is_consecutive = 1 then customer_id end) as consecutive_customer,
        count(distinct case when is_churn = 1 then customer_id end) as churn_customer
    from customer_flag
    group by 1
),
dim_month as(
        select distinct date_trunc(date,Month) as month
        from {{ref('dim_date')}}
)
select
    m.month,
    COALESCE(a.active_customer, 0) AS active_customer,
    COALESCE(a.new_customer, 0) AS new_customer,
    COALESCE(a.return_customer, 0) AS return_customer,
    COALESCE(a.consecutive_customer, 0) AS consecutive_customer,
    COALESCE(a.churn_customer, 0) AS churn_customer
from dim_month m
left join agg a
on date(m.month) = date(a.month)
order by m.month
with order_base as (
    -- Grain: 1 row / order
    select
        customer_id,
        order_id,
        order_date
    from {{ ref('dim_order') }}
),

order_reason_base as (
    -- Map order ↔ reason
    select
        ob.customer_id,
        ob.order_id,
        ob.order_date,
        sor.reason_name,
        sor.reason_id,
        sor.reason_type
    from order_base ob
    left join {{ ref('fact_sales_order_reason') }} sor
        on ob.order_id = sor.order_id
),

-- =========================
-- 1. CUSTOMER REPEAT RATE
-- =========================
customer_repeat_flag as (
    select
        customer_id,
        case when count(distinct order_id) > 1 then true else false end as is_repeat_customer
    from order_base
    group by customer_id
),

customer_reason_map as (
    select distinct
        customer_id,
        reason_name
    from order_reason_base
),

reason_repeat_customer_rate as (
    select
        crm.reason_name,
        count(distinct crm.customer_id) as total_customers,
        count(distinct case when cr.is_repeat_customer then crm.customer_id end) as repeat_customers,
        round(
            count(distinct case when cr.is_repeat_customer then crm.customer_id end) * 1.0
            / count(distinct crm.customer_id),
            2
        ) as repeat_customer_rate
    from customer_reason_map crm
    left join customer_repeat_flag cr
        on crm.customer_id = cr.customer_id
    group by crm.reason_name
),

-- =========================
-- 2. NEXT ORDER (CORRECT GRAIN)
-- =========================
order_with_next as (
    select
        customer_id,
        order_id,
        order_date,
        lead(order_id) over (partition by customer_id order by order_date) as next_order_id,
        lead(order_date) over (partition by customer_id order by order_date) as next_order_date
    from order_base
),

order_reason_with_next as (
    select
        own.*,
        orb.reason_name
    from order_with_next own
    left join order_reason_base orb
        on own.order_id = orb.order_id
),

-- =========================
-- 3. SAME REASON (IMMEDIATE NEXT)
-- =========================
order_same_reason_flag as (
    select
        a.customer_id,
        a.order_id,
        a.reason_name,
        a.order_date,
        a.next_order_id,
        a.next_order_date,
        case when b.reason_name is not null then true else false end as is_same_reason_next
    from order_reason_with_next a
    left join order_reason_base b
        on a.next_order_id = b.order_id
       and a.reason_name = b.reason_name
),

reason_same_reason_rate as (
    select
        reason_name,
        count(distinct order_id) as total_orders,
        count(distinct case when is_same_reason_next then order_id end) as same_reason_orders,
        round(
            count(distinct case when is_same_reason_next then order_id end) * 1.0
            / count(distinct order_id),
            2
        ) as same_reason_next_order_rate
    from order_same_reason_flag
    where next_order_id is not null
    group by reason_name
),

-- =========================
-- 4. TIME TO NEXT ORDER
-- =========================
reason_avg_days_to_next_order as (
    select
        orb.reason_name,
        avg(date_diff(own.next_order_date, own.order_date, day)) as avg_days_to_next_order
    from order_with_next own
    left join order_reason_base orb
        on own.order_id = orb.order_id
    where own.next_order_date is not null
    group by orb.reason_name
),

-- =========================
-- 5. FIRST ORDER REASON
-- =========================
customer_first_order as (
    select *
    from (
        select *,
               row_number() over(partition by customer_id order by order_date, order_id) as rn
        from order_base
    )
    where rn = 1
),

reason_first_order_count as (
    select
        orb.reason_name,
        count(distinct cfo.order_id) as first_order_count
    from customer_first_order cfo
    left join order_reason_base orb
        on cfo.order_id = orb.order_id
    group by orb.reason_name
),

-- =========================
-- 6. NEXT SAME REASON (ANYTIME)
-- =========================
order_next_same_reason_anytime as (
    select
        a.customer_id,
        a.order_id,
        a.reason_name,
        a.order_date,
        min(b.order_date) as next_same_reason_date
    from order_reason_base a
    left join order_reason_base b
        on a.customer_id = b.customer_id
       and a.reason_name = b.reason_name
       and b.order_date > a.order_date
    group by a.customer_id, a.order_id, a.reason_name, a.order_date
),

reason_avg_days_to_next_same_reason as (
    select
        reason_name,
        avg(date_diff(next_same_reason_date, order_date, day)) as avg_days_to_next_same_reason
    from order_next_same_reason_anytime
    where next_same_reason_date is not null
    group by reason_name
),

-- =========================
-- 7. REVENUE METRICS
-- =========================
order_revenue as (
    select
        _id as order_id,
        sum(line_total) as order_revenue
    from {{ ref('fact_order') }}
    group by _id
),

reason_revenue_metrics as (
    select
        orb.reason_name,
        sum(orv.order_revenue) as total_revenue,
        count(distinct orb.order_id) as total_orders,
        avg(orv.order_revenue) as avg_revenue_per_order
    from order_reason_base orb
    left join order_revenue orv
        on orb.order_id = orv.order_id
    group by orb.reason_name
),
reason_dim as(
    select distinct
        reason_id,
        reason_name,
        reason_type
    from {{ ref('fact_sales_order_reason') }}
),

-- =========================
-- FINAL MART
-- =========================
final as (
    select
        rd.reason_id,
        rd.reason_name,
        rd.reason_type,

        rr.repeat_customer_rate,
        sr.same_reason_next_order_rate,

        atn.avg_days_to_next_order,
        ats.avg_days_to_next_same_reason,

        fo.first_order_count,

        rev.total_revenue,
        rev.total_orders,
        rev.avg_revenue_per_order

    from reason_dim rd

    left join reason_repeat_customer_rate rr
        on rd.reason_name = rr.reason_name

    left join reason_same_reason_rate sr
        on rd.reason_name = sr.reason_name

    left join reason_avg_days_to_next_order atn
        on rd.reason_name = atn.reason_name

    left join reason_avg_days_to_next_same_reason ats
        on rd.reason_name = ats.reason_name

    left join reason_first_order_count fo
        on rd.reason_name = fo.reason_name

    left join reason_revenue_metrics rev
        on rd.reason_name = rev.reason_name
)

select *
from final

-- Nếu có thể với lí do mua hàng đầu tiên sẽ dẫn đến lí do mua hàng nào tiếp theo
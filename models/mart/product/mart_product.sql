with product_metrics as(
    select
        product_id,
        sum(line_total) as revenue,
        sum(standard_cost * order_quantity) as cost,
        sum(order_quantity) as sold_quantity,
        sum(line_total) - sum(standard_cost * order_quantity) as profit,
        (sum(line_total) - sum(standard_cost * order_quantity)) / sum(line_total) as profit_margin,
        count(distinct _id) as total_order,
        count(distinct
                case
                    when is_online = '1' then _id
                end
        ) as online_purchase,
        count(distinct
                case
                    when is_online = '0' then _id
                end
        ) as offline_purchase
    from {{ref('fact_order')}} fod
    group by 1
),
repurchase_product as(
    select
        product_id,
        count(distinct customer_id) as total_customer,
        count(distinct case when cnt > 1 then customer_id end) as repeat_customer,
        count(distinct case when cnt > 1 then customer_id end) / count(distinct customer_id) as repeat_rate_customer
    from(
        select
            fod.product_id,
            dod.customer_id,
            count(*) as cnt
        from {{ref('fact_order')}} fod
        left join {{ref('dim_order')}} dod
        on fod._id = dod.order_id
        group by 1,2
    )  as t
    group by 1
),
base as(
    select
        fod.order_date,
        fod.product_id,
        dod.customer_id
    from {{ref('fact_order')}} fod
    left join {{ref('dim_order')}} dod
    on fod._id = dod.order_id
),
next_order_per_customer as(
    select *,
            lead(order_date) over (partition by customer_id,product_id order by order_date) as next_order_date
    from base
),
avg_next_order_with_same_product as(
    select
        product_id,
        avg(date_diff(next_order_date,order_date,day)) as avg_time_to_repurchase
    from next_order_per_customer
    group by 1
),
lastest_purchase_per_product as(
    select
        product_id,
        max(order_date) as lastest_purchase
    from base
    group by 1
),
day_from_lastest_day as(
    select
        product_id,
        date_diff((select max(order_date) from base),lastest_purchase,day) as datediff_from_last_purchase
    from lastest_purchase_per_product
)
select
    p._id as product_id,
    p.product_number,
    p.product_model_id,
    p.product_subcategory_id,
    p.category_name,
    p.subcategory_name,
    p.product_name,
    p.description,
    p.size,
    p.class,
    p.color,
    p.style,
    p.weight,
    p.is_manufactured,
    p.price,
    p.standard_cost,
    p.type,
    p.start_date,
    p.end_date,
    p.end_manufacted_date,
    p.min_inventory_quantity,
    p.reorder_point_quantity,
    p.status,
    p.manufacture_days,
    p.avg_rating,
    pm.revenue,
    pm.cost,
    pm.sold_quantity,
    pm.profit,
    pm.profit_margin,
    pm.total_order,
    pm.online_purchase,
    pm.offline_purchase,
    rp.repeat_customer,
    rp.repeat_rate_customer,
    a.avg_time_to_repurchase,
    b.datediff_from_last_purchase
from {{ref('dim_product')}} as p
left join product_metrics pm
    on p._id = pm.product_id
left join repurchase_product rp
    on rp.product_id = p._id
left join avg_next_order_with_same_product a
    on a.product_id = p._id
left join day_from_lastest_day b
    on b.product_id = p._id
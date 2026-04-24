-- Product Price History đang mismatch với UnitPrice trong bảng SalesOrderDetail.
-- Suppose: Miss thông tin lịch sử price trong bảng price history
-- Hoặc là đàm phán hoặc giảm giá do nhân viên tự cung cấp giá sales price
-- Hiện tại vẫn lấy giữ liệu từ unit price để thống nhất lịch sử bán hàng.
-- Khi update cột mới cần phải full-refresh: dbt run --full-refresh -m int_fact_order
{{
    config(
        materialized = 'incremental',
        unique_key = 'order_detail_id',
        on_schema_change = 'fail',
        sort = ['order_date'],
        incremental_strategy = 'merge'
    )
}}
with cte as(
    select
        soh._id,
        sod._id as order_detail_id,
        soh.order_number,
        sod.carrier_tracking,
        p.product_name,
        p.subcategory_name,
        p.category_name,
        sod.order_quantity,
        sod.unit_price as sell_price,
        case
            when sod.unit_price < pscd2.price then 'DISCOUNTED / NEGOTIATE'
            when sod.unit_price = pscd2.price then 'STANDARD'
        end as price_type,
        p.price as standard_price,
        sod.unit_price_discount,
        sop.offer_type,
        sop.category,
        sop.description,
        sm.ship_base,
        sm.ship_rate,
        sod.line_total,
        soh.sales_amount,
        soh.tax_amount,
        soh.ship_amount,
        soh.total_amount,
        soh.order_date
    from {{ref('stg_sales_order_header')}} as soh
    left join {{ref('stg_sales_order_detail')}} as sod
        on sod.order_id = soh._id
    left join {{ref('stg_ship_method')}} sm
        on sm._id = soh.shipmethod_id
    left join {{ref('int_product')}} p
        on p._id = sod.product_id
    left join {{ref('bridge_special_offer_product')}} sop
        on sop._id = sod.specialoffer_id
        and sop.product_id = sod.product_id
        and soh.order_date between sop.start_date and sop.end_date
        and sod.order_quantity between sop.min_quantity and sop.max_quantity
    left join {{ref('int_product_price_scd2')}} pscd2
        on pscd2.product_id = sod.product_id
        and soh.order_date between pscd2.start_date and pscd2.end_date
    {% if is_incremental()%}
        where soh.order_date >= (select coalesce(max(order_date)) from {{this}})
    {% endif%}
)
select *
from cte

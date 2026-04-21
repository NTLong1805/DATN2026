with cte as(
    select
        soh._id,
        soh.order_number,
        sod.carrier_tracking,
        sod.product_id,
        sod.order_quantity,
        sod.unit_price,
        sod.unit_price_discount,
        sm.ship_base,
        sm.ship_rate,
        sod.line_total,
        soh.sales_amount,
        soh.tax_amount,
        soh.ship_amount,
        soh.total_amount
    from {{ref('stg_sales_order_header')}} as soh
    left join {{ref('stg_sales_order_detail')}} as sod
        on sod.order_id = soh._id
    left join {{ref('stg_ship_method')}} sm
        on sm._id = soh.shipmethod_id
)
select *
from cte

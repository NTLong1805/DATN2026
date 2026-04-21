with cte as(
    select
        a.order_id,
        b.order_line_id,
        a.revision_number,
        b.product_name,
        b.order_quantity,
        b.unit_price,
        b.received_quantity,
        b.rejected_quantity,
        b.stocked_quantity,
        a.ship_date,
        a.tax_amount,
        a.ship_amount,
        a.purchase_amount,
        a.total_amount,
        a.order_date,
    from {{ref('int_purchase_order_header')}} as a
    left join {{ref('int_purchase_order_detail')}} as b
    on a.order_id = b.order_id
)
select *
from cte
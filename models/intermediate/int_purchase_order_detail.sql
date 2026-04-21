with cte as(
    select
        d.order_id,
        d._id as order_line_id,
        d.due_date,
        p.product_number,
        p.product_name,
        d.order_quantity,
        d.unit_price,
        d.stocked_quantity,
        d.received_quantity,
        d.rejected_quantity,
        d.line_total
    from {{ref('stg_purchase_order_detail')}} d
    left join {{ref('int_product')}} p
        on p._id = d.product_id
)
select *
from cte
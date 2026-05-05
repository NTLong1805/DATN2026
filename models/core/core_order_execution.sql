/*
    1 dòng là chi tiết 1 đơn hàng
*/
select
    b.order_id,
    b.revision_number,
    b.order_date,
    b.due_date,
    b.ship_date,
    b.status,
    b.is_online,
    b.purchase_order_number,
    b.account_number,
    c.full_name as employee_name,
    d._id as customer_id,
    d.full_name as customer_name,
    e.territory_name,
    b.bill_address,
    b.ship_address,
    b.shipcompany_name,
    b.creditcard_id,
    b.creditcard_approval,
    b.comment,
    a.carrier_tracking,
    a.product_name,
    a.subcategory_name,
    a.category_name,
    a.order_quantity,
    a.sell_price,
    a.standard_cost,
    a.standard_price,
    a.unit_price_discount,
    a.price_type,
    a.offer_type,
    a.category as specieal_offer,
    a.category as special_offer_category,
    a.description special_offer_description,
    a.ship_base,
    a.ship_rate,
    a.line_total,
    a.sales_amount,
    a.tax_amount,
    a.ship_amount,
from  {{ref('fact_order')}} a
left join {{ref('dim_order')}} b
    on a._id = b.order_id
left join {{ref('dim_employee')}} c
    on c.employee_id = b.salesperson_id
left join {{ref('dim_customer')}} d
    on d._id = b.customer_id
left join {{ref('dim_sales_territory')}} e
    on e._id = b.territory_id
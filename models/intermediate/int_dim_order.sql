with cte as(
    select
        soh._id as order_id,
        soh.revision_number,
        soh.order_date,
        soh.due_date,
        soh.ship_date,
        soh.status,
        soh.is_online,
        soh.purchase_order_number,
        soh.account_number,
        soh.salesperson_id,
        e.full_name as employee_name,
        soh.customer_id,
        c.full_name as customer_name,
        st._id as territory_id,
        st.territory_name,
        ad1.main_address as bill_address,
        ad2.main_address as ship_address,
        sm.shipcompany_name,
        soh.creditcard_id,
        soh.creditcard_approval,
        soh.comment,
        total_amount
    from {{ref('stg_sales_order_header')}} as soh
--     left join {{ref('stg_sales_order_detail')}} as sod
--      on sod.order_id = soh._id
--     left join {{ref('stg_sales_order_header_sales_reason')}} s
--         on s.order_id = sod.order_id
--     left join {{ref('stg_sales_reason')}} sr
--         on s.reason_id = sr._id
--     left join {{ref('stg_special_offer')}} so
--         on sod.specialoffer_id = so._id
--     left join {{ref('stg_product')}} pr
--         on sod.product_id = pr._id
    left join {{ref('int_customer')}} c
        on c._id = soh.customer_id
    left join {{ref('int_employee')}} e
        on e.employee_id = soh.salesperson_id
    left join {{ref('stg_sales_territory')}} st
        on st._id = soh.territory_id
    left join {{ref('stg_ship_method')}} sm
        on sm._id = soh.shipmethod_id
    left join {{ref('int_address')}} ad1
        on soh.bill_address_id = ad1._id
    left join {{ref('int_address')}} ad2
        on ad2._id = soh.ship_address_id
)
select *
from cte

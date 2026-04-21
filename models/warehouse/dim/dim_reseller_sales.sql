with cte as(
    select
        order_id,
        revision_number,
        status,
        employee_name,
        department_name,
        vendor_account_number,
        vendor_name,
        shipcompany_name,
        order_date,
        ship_date
    from {{ref('int_purchase_order_header')}}
)
select *
from cte
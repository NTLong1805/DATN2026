with cte as(
    select
        p._id as order_id,
        p.revision_number,
        case
            when p.status = '1' then 'Pending'
            when p.status = '2' then 'Approved'
            when p.status = '3' then 'Rejected'
            when p.status = '4' then 'Compelte'
        end as status,
        p.employee_id,
        e.full_name as employee_name,
        e.department_name,
        e.department_group_name,
        v.account_number as vendor_account_number,
        v.Name as vendor_name,
        sm.shipcompany_name,
        p.order_date,
        p.ship_date,
        p.tax_amount,
        p.ship_amount,
        p.purchase_amount,
        p.total_amount
    from {{ref('stg_purchase_order_header')}} p
    left join {{ref('int_employee')}} e
        on e.employee_id = p.employee_id
    left join {{ref('int_vendor')}} v
        on p.vendor_id = v._id
    left join {{ref('int_ship_method')}} sm
        on sm._id = p.ship_method_id
)
select *
from cte
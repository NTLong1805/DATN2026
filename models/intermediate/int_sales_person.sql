select e.*,
       sp.territory_id,
       sp.sales_quota_amount,
       sp.bonus_amount,
       sp.commission_amount,
       sp.sales_ytd,
       sp.sales_last_year
from {{ref('stg_sales_person')}} sp
left join {{ref('int_employee')}} e
on e.employee_id = sp._id
with final as(
    select _id,
           department_id,
           shift_id,
           start_date,
           lead(start_date) over (partition by _id order by start_date) as end_date,
           case
               when lead(start_date) over (partition by _id order by start_date) is null then True
               else False
           end as is_current
    from {{ref('stg_employee_department_history')}}
)
select *
from final
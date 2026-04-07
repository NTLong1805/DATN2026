with final as(
    select _id,
           salary,
           pay_frequency,
           rate_change_date as start_date,
           lead(rate_change_date) over (partition by _id order by rate_change_date) as end_date,
           case
               when lead(rate_change_date) over (partition by _id order by rate_change_date)  is null then True
               else False
           end as is_current
    from {{ref('stg_employee_pay_history')}}
)
select *
from final

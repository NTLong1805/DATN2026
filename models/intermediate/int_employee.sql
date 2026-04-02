with rankLastDepartment as(
    select *,
           rank() over(partition by _id order by start_date desc) as rn
    from {{ref('stg_employee_department_history')}}
),
latestDepartment as(
    select
        _id as employee_id,
        department_id,
        shift_id,
        start_date,
        end_date
    from rankLastDepartment
    where rn = 1
),
rankLastRate as (
    select
        *,
        rank() over (partition by _id order by rate_change_date desc) as rn
    from {{ref('stg_employee_pay_history')}}
),
latestRate as(
    select
        _id as employee_id,
        salary,
        pay_frequency,
        rate_change_date
    from rankLastRate
    where rn = 1
),
employee as(
    select
        e._id as employee_id,
        e.national_id,
        e.organization_level,
        REGEXP_REPLACE(e.path_node, r'[^/]+/$', '') AS manager_node,
        path_node,
        e.job_title,
        case
            when cast(name_style as int64) = 0 then concat(coalesce(p.last_name,' '),' ',coalesce(p.middle_name,' '),' ',coalesce(p.first_name,' '))
            when cast(name_style as int64) = 1 then concat(coalesce(p.first_name,' '),' ',coalesce(p.middle_name,' '),' ',coalesce(p.last_name,' '))
            end
        as full_name,
        p.person_type,
        e.birth_date,
        e.martial_status,
        e.sex,
        e.hire_date,
        e.is_offical,
        e.holiday_hour,
        e.absent_hour,
        e.status,
        d.department_name,
        d.department_group_name,
        ld.start_date,
        ld.end_date,
        ld.shift_id,
        lr.salary,
        lr.pay_frequency,
        lr.rate_change_date
    from {{ref('stg_employee')}} e
    left join {{ref('stg_person')}} as p
        on e._id = p._id
    left join latestDepartment as ld
        on ld.employee_id = e._id
    left join {{ref('stg_department')}} as d
        on d._id = ld.department_id
    left join latestRate lr
        on lr.employee_id = e._id
),
employee_manager as(
    select e.*,
        case
            when manager_node = '/' then '1'
            else se._id
        end as manager_id
    from employee as e
    left join {{ref('stg_employee')}} as se
        on se.path_node = e.manager_node
)
select *
from employee_manager
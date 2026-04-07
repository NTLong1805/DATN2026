with latestDepartment as(
    select
        _id as employee_id,
        department_id,
        shift_id,
        start_date,
        end_date,
        is_current
    from {{ref('int_employee_department_scd2')}}
    where is_current is true
),

latestRate as(
    select
        _id as employee_id,
        salary,
        pay_frequency,
        start_date,
        end_date,
        is_current
    from {{ref('int_employee_pay_scd2')}}
    where is_current is true
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
        ld.shift_id,
        lr.salary,
        lr.pay_frequency,
        lr.is_current
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
select
    employee_id,
    national_id,
    organization_level as level,
    job_title,
    full_name,
    person_type,
    birth_date,
    martial_status,
    sex,
    hire_date,
    is_offical,
    holiday_hour,
    absent_hour,
    status,
    department_name,
    department_group_name,
    shift_id,
    salary,
    pay_frequency,
    manager_id
from employee_manager
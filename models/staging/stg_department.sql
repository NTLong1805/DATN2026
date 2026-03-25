with src as(
    select *
    from {{source('DATN_RAW','department')}}
)
select
    cast(_DepartmentID as string) as _id,
    Name as department_name,
    GroupName as department_group_name
from src
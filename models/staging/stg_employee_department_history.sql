with src as(
    select *
    from {{source('DATN_RAW','EmployeeDepartmentHistory')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(DepartmentID as string) as department_id,
    cast(ShiftID as string) as shift_id,
    StartDate::date as start_date,
    EndDate::date as end_date,
    ModifiedDate::timestamp as _ts
from src
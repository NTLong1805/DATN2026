with src as(
    select *
    from {{source('DATN_RAW','EmployeeDepartmentHistory')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(DepartmentID as string) as department_id,
    cast(ShiftID as string) as shift_id,
    safe_cast(StartDate as timestamp) as start_date,
    safe_cast(EndDate as timestamp) as end_date,
    safe_cast(ModifiedDate as timestamp) as _ts
from src
with src as(
    select *
    from {{source('DATN_RAW','Shift')}}
)
select
    cast(_ShiftID as string) as _id,
    Name as shift_name,
    safe_cast(StartTime as datetime) as start_time,
    safe_cast(EndTime as datetime) as end_time,
    cast(ModifiedDate as timestamp) as _ts
from src
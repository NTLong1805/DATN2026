with src as(
    select *
    from {{source('DATN_RAW','Shift')}}
)
select
    cast(_ShiftID as string) as _id,
    Name as shift_name,
    cast(StartTime as timestamp) as start_time,
    cast(EndTime as timestamp) as end_time,
    cast(ModifiedDate as timestamp) as _ts
from src
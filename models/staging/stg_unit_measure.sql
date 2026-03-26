with src as(
    select *
    from {{source('DATN_RAW','UnitMeasure')}}
)
select
    cast(_UnitMeasureCode as string) as _id,
    Name as name
from src
with src as(
    select *
    from {{source('DATN_RAW','ContactType')}}
)
select
    cast(_ContactTypeID as string) as _id,
    Name as ContactName,
    ModifiedDate::timestamp as _ts
from src
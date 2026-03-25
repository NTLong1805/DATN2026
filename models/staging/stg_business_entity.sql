with src as(
    select *
    from {{source('DATN_RAW','BusinessEntity')}}
)
select
    cast(_BusinessEntityID as string),
    ModifiedDate::timestamp as _ts
from src
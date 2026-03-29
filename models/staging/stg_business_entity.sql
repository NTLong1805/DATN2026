with src as(
    select *
    from {{source('DATN_RAW','BusinessEntity')}}
)
select
    cast(_BusinessEntityID as string) as entity_id,
    cast(ModifiedDate as timestamp) as _ts
from src
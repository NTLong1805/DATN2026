with src as(
    select *
    from {{source('DATN_RAW','BusinessEntity')}}
)
select
    cast(_BusinessEntityID as string) as person_id,
    cast(ModifiedDate as timestamp) as _ts
from src
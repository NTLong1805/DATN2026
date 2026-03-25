with src as(
    select *
    from {{source('DATN_RAW','Address')}}
)
select
    cast(_AddressID as string) as _id,
    PostalCode,
    AddressLine1,
    AddressLine2,
    cast(ModifiedDate as timestamp) as _ts as _ts
from src
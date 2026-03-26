with src as(
    select *
    from {{source('DATN_RAW','Address')}}
)
select
    cast(_AddressID as string) as _id,
    cast(StateProvinceID as string) as state_province_id,
    PostalCode,
    AddressLine1,
    AddressLine2,
    cast(ModifiedDate as timestamp) as _ts
from src
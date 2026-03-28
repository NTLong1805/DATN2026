with src as(
    select *
    from {{source('DATN_RAW','Address')}}
)
select
    cast(_AddressID as string) as _id,
    cast(StateProvinceID as string) as state_province_id,
    City as city,
    PostalCode as postal_code,
    AddressLine1 as main_address,
    AddressLine2,
    cast(ModifiedDate as timestamp) as _ts
from src
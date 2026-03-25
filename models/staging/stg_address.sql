with src as(
    select *
    from {{source('DATN_RAW','Address')}}
)
select
    _AddressID as _id,
    PostalCode,
    AddressLine1,
    AddressLine2,
    ModifiedDate as _ts
from src
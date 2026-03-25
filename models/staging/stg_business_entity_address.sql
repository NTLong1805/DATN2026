with src as(
    select *
    from {{source('DATN_RAW','BusinessEntityAddress')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(AddressTypeID as string) as AddressTypeID,
    cast(AddressID as string) as AddressID,
    cast(ModifiedDate as timestamp) as _ts
from src
with src as(
    select *
    from {{source('DATN_RAW','BusinessEntityContact')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(ContactTypeID as string) as ContactTypeID,
    cast(ContactID as string) as ContactID,
    ModifiedDate::timestamp as _ts
from src
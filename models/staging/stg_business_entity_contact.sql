with src as(
    select *
    from {{source('DATN_RAW','BusinessEntityContact')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(PersonID as string) as person_id,
    cast(ContactTypeID as string) as ContactTypeID,
    cast(ModifiedDate as timestamp) as _ts
from src
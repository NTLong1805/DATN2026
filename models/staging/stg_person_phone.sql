with src as(
    select *
    from {{source('DATN_RAW','PersonPhone')}}
)
select
    cast(_BusinessEntityID as string) as person_id,
    cast(PhoneNumberTypeID as string) as phone_number_type_id,
    PhoneNumber as phone_number,
    cast(ModifiedDate as timestamp) as _ts
from src
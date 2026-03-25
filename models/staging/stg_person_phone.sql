with src as(
    select *
    from {{source('DATN_RAW','PersonPhone')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(PhoneNumberTypeID as string) as phone_number_type_id,
    PhoneNumnber as phone_number,
    ModifiedDate::timestampt as _ts
from src
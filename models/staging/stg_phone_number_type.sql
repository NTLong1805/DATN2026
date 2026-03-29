with src as(
    select *
    from {{source('DATN_RAW','PhoneNumberType')}}
)
select
    cast(_PhoneNumberTypeID as string) as _id,
    Name as phone_number_type,
    cast(ModifiedDate as timestamp) as _ts
from src
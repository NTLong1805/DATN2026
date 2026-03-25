with src as(
    select *
    from {{source('DATN_RAW','PhoneNumberType')}}
)
select
    cast(_PhoneNumberTypeID as string) as _id,
    Name as type_name,
    ModifiedDate::timestamp as _ts
from src
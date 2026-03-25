with src as (
    select *
    from {{source('DATN_RAW','EmailAddress')}}
)
select
    Cast(EmailAddressID) as _id,
    EmailAddress as email_address,
    ModifiedDate::timestamp as _ts
from src
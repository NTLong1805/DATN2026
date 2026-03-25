with src as (
    select *
    from {{source('DATN_RAW','EmailAddress')}}
)
select
    Cast(EmailAddressID) as _id,
    EmailAddress as email_address,
    cast(ModifiedDate as timestamp) as _ts
from src
with src as (
    select *
    from {{source('DATN_RAW','EmailAddress')}}
)
select
    Cast(EmailAddressID as string) as _id,
    cast(_BusinessEntityID as string) as person_id,
    EmailAddress as email_address,
    cast(ModifiedDate as timestamp) as _ts
from src
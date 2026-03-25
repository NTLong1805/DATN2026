with src as(
    select *
    from {{source('DATN_RAW','PersonCreditCard')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(CreditCardID as string) as credit_card_id,
    cast(ModifiedDate as timestamp) as _ts
from src
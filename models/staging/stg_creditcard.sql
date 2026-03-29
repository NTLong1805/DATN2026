with src as(
    select *
    from {{source('DATN_RAW','CreditCard')}}
)
select
    cast(_CreditCardID as string) as _id,
    CardNumber as card_num,
    CardType as card_type,
    ExpMonth as exp_month,
    ExpYear as exp_year,
    cast(ModifiedDate as timestamp) as _ts
from src
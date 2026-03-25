with src as(
    select *
    from {{source('DATN_RAW','CurrencyRate')}}
)
select
    cast(_CurrencyCode as string) as _id,
    Name as CurrunceyName,
    ModifiedDate::timestamp as _ts
from src
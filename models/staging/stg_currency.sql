with src as(
    select *
    from {{source('DATN_RAW','Currency_csv')}}
)
select
    cast(_CurrencyCode as string) as _id,
    Name as currency_name,
    cast(ModifiedDate as timestamp) as _ts
from src
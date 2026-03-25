with src as(
    select *
    from {{source('DATN_RAW','CountryRegionCurrency_csv')}}
)
select
    cast(_CountryRegionCode as string) as CountryRegionCode,
    cast(CurrencyCode as string) as _id,
    cast(ModifiedDate as timestamp) as _ts
from src
with src as(
    select *
    from {{src('DATN_RAW','CountryRegionCurrency_csv')}}
)
select
    cast(_CountryRegionCode as string) as CountryRegionCode,
    cast(CurrencyCode as string) as _id,
    ModifiedDate::timestamp as _ts
from src
with src as(
    select *
    from {{ref('DATN_RAW','CountryRegion')}}
)
select
    cast(_CountryRegionCode as string) as _id,
    Name as CountryRegionName,
    ModifiedDate::timestamp as _ts
from src
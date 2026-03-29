with src as(
    select *
    from {{source('DATN_RAW','CountryRegion')}}
)
select
    cast(_CountryRegionCode as string) as _id,
    Name as country,
    cast(ModifiedDate as timestamp) as _ts
from src
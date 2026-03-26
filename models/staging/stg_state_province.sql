with src as(
    select *
    from {{source('DATN_RAW','StateProvince')}}
)
select
    cast(_StateProvinceID as string) as _id,
    StateProvinceCode as state_province_code,
    cast(CountryRegionCode as string) as country_region_code,
    IsOnlyStateProvinceFlag as isTrue,
    Name as state_province_name,
    cast(TerritoryID as string) as territory_id,
    cast(ModifiedDate as timestamp) as _ts
from src
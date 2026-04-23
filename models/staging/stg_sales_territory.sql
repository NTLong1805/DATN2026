with src as(
    select *
    from {{source('DATN_RAW','SalesTerritory')}}
)
select
    cast(_TerritoryID as string) as _id,
    Name as territory_name,
    cast(CountryRegionCode as string) as country_region_code,
    `Group` as continent,
    cast(SalesYTD as float64) as sales_ytd,
    cast(SalesLastYear as float64) as sales_last_year,
    cast(CostYTD as float64) as cost_ytd,
    cast(CostLastYear as float64) as cost_last_year,
    cast(ModifiedDate as timestamp) as _ts
from src
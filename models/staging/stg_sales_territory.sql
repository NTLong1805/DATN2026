with src as(
    select *
    from {{source('DATN_RAW','SalesTerritory')}}
)
select
    cast(_TerritoryID as string) as _id,
    Name as territory_name,
    cast(CountryRegionCode as string) as country_region_code,
    `Group` as continent,
    SalesYTD as sales_ytd,
    SalesLastYear as sales_last_year,
    CostYTD as cost_ytd,
    CostLastYear as cost_last_year,
    cast(ModifiedDate as timestamp) as _ts
from src
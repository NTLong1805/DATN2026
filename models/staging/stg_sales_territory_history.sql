with src as(
    select *
    from {{source('DATN_RAW','SalesTerritoryHistory')}}
)
select
    cast(_BusinessEntityID as string) as salesperson_id,
    cast(TerritoryID as string) as territory_id,
    cast(StartDate as timestamp) as start_date,
    cast(EndDate as timestamp) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
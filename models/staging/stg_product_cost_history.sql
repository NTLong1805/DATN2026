with src as(
    select *
    from {{source('DATN_RAW','ProductCostHistory')}}
)
select
    cast(_ProductID as string) as product_id,
    cast(StandardCost as float64) as standard_cost,
    cast(StartDate as timestamp) as start_date,
    safe_cast(EndDate as timestamp) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
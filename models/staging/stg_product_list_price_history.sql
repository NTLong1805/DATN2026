with src as(
    select *
    from {{source('DATN_RAW','ProductListPriceHistory')}}
)

select
    cast(_ProductID as string) as product_id,
    ListPrice as list_price,
    safe_cast(StartDate as timestamp) as start_date,
    safe_cast(EndDate as timestamp) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
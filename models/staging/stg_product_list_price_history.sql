with src as(
    select *
    from {{source('DATN_RAW','ProductListPriceHistory')}}
)

select
    cast(_ProductID as string) as product_id,
    ListPrice as list_price,
    cast(StartDate as date) as start_date,
    cast(EndDate as date) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
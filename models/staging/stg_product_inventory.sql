with src as(
    select *
    from {{source('DATN_RAW','ProductInventory')}}
)
select
    cast(_ProductID as string) as product_id,
    cast(LocationID as string) as location_id,
    Bin as bin,
    Shelf as shelf,
    cast(ModifiedDate as timestamp) as _ts
from src

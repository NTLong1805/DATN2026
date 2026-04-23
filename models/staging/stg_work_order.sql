with src as(
    select *
    from {{source('DATN_RAW','WorkOrder')}}
)
select
    cast(_WorkOrderID as string) as _id,
    ScrapReasonID as scrap_reason_id,
    cast(ProductID as string) as product_id,
    cast(OrderQty as float64) as order_quantity,
    cast(StockedQty as float64) as stocked_quantity, -- Stocked = Order - Scarpped
    cast(ScrappedQty as float64) as scrapped_quantity,
    Cast(StartDate as timestamp) as start_date,
    Cast(EndDate as timestamp) as end_date,
    cast(DueDate as timestamp) as due_date
from src
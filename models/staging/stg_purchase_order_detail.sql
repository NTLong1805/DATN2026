with src as(
    select *
    from {{source('DATN_RAW','PurchaseOrderDetail')}}
)
select
    cast(_PurchaseOrderID as string) as order_id,
    cast(PurchaseOrderDetailID as string) as _id,
    cast(DueDate as timestamp) as due_date,
    Cast(ProductID as string) as product_id,
    cast(OrderQty as float64) as order_quantity,
    cast(UnitPrice as float64) as unit_price,
    cast(StockedQty as float64) as stocked_quantity,
    cast(ReceivedQty as float64) as received_quantity,
    cast(RejectedQty as float64) as rejected_quantity,
    cast(LineTotal as float64) as line_total,
    cast(ModifiedDate as timestamp) as _ts
from src
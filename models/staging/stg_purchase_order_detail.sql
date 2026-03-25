with src as(
    select *
    from {{source('DATN_RAW','PurchaseOrderDetail')}}
)
select
    cast(_PurchaseOrderID as string) as purchase_orderid,
    cast(PurchaseOrderDetailID as string) as _id,
    cast(DueDate as timestamp) as due_date,
    Cast(ProductID as string) as product_id,
    OrderQty as order_quantity,
    UnitPrice as unit_price,
    StockedQty as stocked_quantity,
    ReceivedQty as received_quantity,
    RejectedQty as rejected_quantity,
    LineTotal as line_total,
    cast(ModifiedDate as timestamp) as _ts
from src
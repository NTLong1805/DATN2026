with src as(
    select *
    from {{source('DATN_RAW','ProductVendor')}}
)
select
    cast(BusinessEntityID as string) as _id,
    cast(_ProductID as string) as product_id,
    StandardPrice as standard_price,
    AverageLeadTime as average_leadtime,
    LastReceiptCost as last_receipt_cost,
    MaxOrderQty as max_order_quantity,
    MinOrderQty as min_order_quantity,
    OnOrderQty as on_order_quantity,
    cast(ModifiedDate as timestamp) as _ts
from src
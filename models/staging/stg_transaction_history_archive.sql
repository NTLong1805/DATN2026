with src as(
    select *
    from {{source('DATN_RAW','TransactionHistoryArchive')}}
)
select
    cast(_TransactionID as string) as _id,
    cast(ProductID as string) as product_id,
    cast(ReferenceOrderID as string) as order_id,
    cast(ReferenceOrderLineID as string) as order_number,
    cast(TransactionDate as timestamp) as order_date,
    TransactionType as transaction_type,
    Quantity as quantity,
    ActualCost as cost_amount
from src
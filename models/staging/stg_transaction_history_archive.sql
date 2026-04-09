with src as(
    select *
    from {{source('DATN_RAW','TransactionHistoryArchive')}}
)
select
    cast(ReferenceOrderLineID as string) as order_number,
    cast(ReferenceOrderID as string) as order_id,
    cast(_TransactionID as string) as _id,
    cast(ProductID as string) as product_id,
    cast(TransactionDate as timestamp) as order_date,
    TransactionType as transaction_type,
    cast(Quantity as int) as quantity,
    cast(ActualCost as FLOAT64) as cost_amount
from src
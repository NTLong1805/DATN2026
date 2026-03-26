with src as(
    select *
    from {{source('DATN_RAW','SalesOrderDetail')}}
)
select
    cast(_SalesOrderID as string) as order_id,
    cast(SalesOrderDetailID as string) as _id,
    CarrierTrackingNumber as carrier_tracking,
    cast(ProductID as string) as product_id,
    OrderQty as order_quantity,
    cast(SpecialOfferID as string) as specialoffer_id,
    UnitPrice as unit_price,
    UnitPriceDiscount as unit_price_discount,
    LineTotal as line_total,
    cast(ModifiedDate as timestamp) as _ts
from src
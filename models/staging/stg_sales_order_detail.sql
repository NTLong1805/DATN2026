with src as(
    select *
    from {{source('DATN_RAW','SalesOrderDetail')}}
)
select
    cast(_SalesOrderID as string) as order_id,
    cast(SalesOrderDetailID as string) as _id,
    CarrierTrackingNumber as carrier_tracking,
    cast(ProductID as string) as product_id,
    cast(OrderQty as float64) as order_quantity,
    cast(SpecialOfferID as string) as specialoffer_id,
    cast(UnitPrice as float64) as unit_price,
    cast(UnitPriceDiscount as float64) as unit_price_discount,
    cast(LineTotal as float64) as line_total,
    cast(ModifiedDate as timestamp) as _ts
from src
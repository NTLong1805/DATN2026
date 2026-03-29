with src as(
    select *
    from {{source('DATN_RAW','SalesOrderHeaderSalesReason')}}
)
select
    cast(_SalesOrderID as string) as order_id,
    cast(SalesReasonID as string) as reason_id
from src
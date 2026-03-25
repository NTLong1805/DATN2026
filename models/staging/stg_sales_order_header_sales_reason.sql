with src as(
    select *
    from {{source('DATN_RAW','SalesOrderHeaderSalesReason')}}
)
select
    cast(_SalesOrderID as string) as sales_orderid,
    cast(SalesReasonID as string) as sales_reasonid
from src
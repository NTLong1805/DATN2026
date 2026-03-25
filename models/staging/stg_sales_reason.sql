with src as(
    select *
    from {{source('DATN_RAW','SalesReason')}}
)
select
    cast(_SalesReasonID as string) as _id,
    Name as reason_name,
    ReasonType as reason_type,
    cast(ModifiedDate as timestamp) as _ts
from src
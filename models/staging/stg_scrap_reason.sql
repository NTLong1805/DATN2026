with src as(
    select *
    from {{source('DATN_RAW','ScrapReason')}}
)
select
    cast(_ScrapReasonID as string) as _id,
    Name as scrap_name,
    cast(ModifiedDate as timestamp) as _ts
from src
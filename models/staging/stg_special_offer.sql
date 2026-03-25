with src as(
    select *
    from {{source('DATN_RAW','SpecialOfferProduct')}}
)
select
    cast(_SpecialOfferID as string) as _id,
    Type as offer_type,
    Description as desc,
    Category as category,
    DiscountPct as discount_pct,
    MaxQty as max_quantity,
    MinQty as min_quantity,
    Cast(StartDate as timestamp) as start_date,
    cast(EndDate as timestamp) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
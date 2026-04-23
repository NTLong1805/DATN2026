with src as(
    select *
    from {{source('DATN_RAW','SpecialOffer')}}
)
select
    cast(_SpecialOfferID as string) as _id,
    Type as offer_type,
    Description as description,
    Category as category,
    safe_cast(DiscountPct as float64) as discount_pct,
    safe_cast(MaxQty as float64) as max_quantity,
    safe_cast(MinQty as float64) as min_quantity,
    Cast(StartDate as timestamp) as start_date,
    cast(EndDate as timestamp) as end_date,
    cast(ModifiedDate as timestamp) as _ts
from src
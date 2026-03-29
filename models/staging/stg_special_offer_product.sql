with src as(
    select *
    from {{source('DATN_RAW','SpecialOfferProduct')}}
)
select
    cast(_SpecialOfferID as string) as specialoffer_id,
    cast(ProductID as string) as product_id,
    cast(ModifiedDate as timestamp) as _ts
from src
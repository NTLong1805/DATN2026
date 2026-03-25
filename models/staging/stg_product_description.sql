with src as(
    select *
    from {{source('DATN_RAW','ProductDescription')}}
)
select
    cast(_ProductDescriptionID as string) as _id,
    Description as description,
    Cast(ModifiedDate as timestamp) as _ts
from src
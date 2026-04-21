with src as(
    select
        Cast(_ProductModelID as string) as _id,
        cast(ProductDescriptionID as string) as description_id,
        cast(CultureID as string) as culture_id
    from {{source('DATN_RAW','ProductModelProductDescriptionCulture')}}
)
select *
from src
with src as(
    select *
    from {{source('DATN_RAW','ProductCategory')}}
)
select
    cast(_ProductCategoryID as string) as _id,
    Name as category_name,
    ModifiedDate::timestamp as _ts
from src
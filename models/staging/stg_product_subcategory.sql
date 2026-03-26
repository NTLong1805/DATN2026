with src as(
    select *
    from {{source('DATN_RAW','ProductSubCategory')}}
)
select
    cast(ProductCategoryID as string) as product_category_id,
    cast(_ProductSubCategoryID as string) as _id,
    Name as subcategory_name,
    cast(ModifiedDate as timestamp) as _ts
from src
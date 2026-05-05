with src as(
    select *
    from {{source('DATN_RAW','ProductReview')}}
)
select
    cast(ReviewID as string) as _id,
    cast(ProductID as string) as product_id,
    cast(ReviewDate as timestamp) as review_date,
    cast(ReviewerName as string) as reviewer_name,
    Email as email,
    Comment as comment,
    cast(Rating as int64) as rating
from src
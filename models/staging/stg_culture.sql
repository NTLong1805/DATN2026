with src as(
    select *
    from {{source('DATN_RAW','Culture')}}
)
select
    cast(_CultureID as string) as _id,
    Name as CultureName,
    cast(ModifiedDate as timestamp) as _ts
from src
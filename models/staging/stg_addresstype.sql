with cte as(
    select *
    from {{ source('DATN_RAW','Addresstype')}}
)
select
    cast(_AddressTypeID as string) as _id,
    Name,
    cast(ModifiedDate as timestamp) as _ts
from cte
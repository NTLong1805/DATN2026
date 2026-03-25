with cte as(
    select *
    from {{ source('DATN_RAW','Addresstype')}}
)
select
    cast(_AddressTypeID as string) as _id,
    Name,
    ModifiedDate as _ts
from cte
with src as (
    select *
    from {{source('DATN_RAW','Customer')}}
)
select
    cast(_CustomerID as string) as _id,
    cast(PersonID as string) as person_id,
    Cast(TerritoryID as string) as territory_id,
    cast(StoreID as string) as store_id,
    AccountNumber as account_number,
    ModifiedDate::timestamp as _ts
from src
with src as(
    select *
    from {{source('DATN_RAW','ShipMethod')}}
)
select
    cast(_ShipMethodID as string) as _id,
    Name as shipmethod_name,
    ShipBase as ship_base,
    ShipRate as ship_rate,
    cast(ModifiedDate as string) as _ts
from src
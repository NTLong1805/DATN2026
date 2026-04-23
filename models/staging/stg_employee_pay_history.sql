with src as(
    select *
    from {{source('DATN_RAW','EmployeePayHistory')}}
)
select
    cast(_BusinessEntityID as string ) as _id,
    cast(Rate as FLOAT64) as salary,
    cast(PayFrequency as float64) as pay_frequency,
    cast(RateChangeDate as timestamp) as rate_change_date,
    cast(ModifiedDate as timestamp) as _ts
from src
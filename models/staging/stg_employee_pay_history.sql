with src as(
    select *
    from {{source('DATN_RAW','EmployeePayHistory')}}
)
select
    cast(_BusinessEntityID as string ) as _id,
    Rate as salary,
    PayFrequency as pay_frequency,
    cast(RateChangeDate as timestamp) as rate_change_date,
    cast(ModifiedDate as timestamp) as _ts
from src
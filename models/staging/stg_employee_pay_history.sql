with src as(
    select *
    from {{source('DATN_RAW','EmployeePayHistory')}}
)
select
    cast(_BusinessEntityID as string ) as _id,
    Rate as salary,
    PayFrequency as pay_frequency,
    RateChangeDate::timestamp as rate_change_date,
    ModifiedDate::timestamp as _ts
from src
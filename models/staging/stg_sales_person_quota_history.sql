with src as(
    select *
    from {{source('DATN_RAW','SalesPersonQuotaHistory')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(QuotaDate as timestamp) as quota_date,
    cast(SalesQuota as float64) as sales_quota,
    cast(ModifiedDate as timestamp) as _ts
from src
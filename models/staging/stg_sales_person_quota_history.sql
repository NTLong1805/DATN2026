with src as(
    select *
    from {{source('DATN_RAW','SalesPersonQuotaHistory')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(QuotaDate as timestamp) as quota_date,
    SalesQuota as sales_quota,
    cast(ModifiedDate as timestamp) as _ts
from src
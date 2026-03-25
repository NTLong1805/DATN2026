with src as(
    select *
    from {{source('DATN_RAW','SalesPerson')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(TerritoryID as string) as territory_id,
    SalesQuota as sales_quota_amount, -- Doanh so du kien cua nam
    Bonus as bonus_amount,
    CommissionPct as commission_amount,
    SalesYTD as sales_ytd,
    SalesLastYear as sales_last_year,
    cast(ModifiedDate as timestamp) as _ts
from src
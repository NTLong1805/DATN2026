with src as(
    select *
    from {{source('DATN_RAW','SalesPerson')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(TerritoryID as string) as territory_id,
    safe_cast(SalesQuota as float64) as sales_quota_amount, -- Doanh so du kien cua nam
    safe_cast(Bonus as float64) as bonus_amount,
    safe_cast(CommissionPct as float64) as commission_amount,
    safe_cast(SalesYTD as float64) as sales_ytd,
    safe_cast(SalesLastYear as float64) as sales_last_year,
    cast(ModifiedDate as timestamp) as _ts
from src
with src as(
    select *
    from {{source('DATN_RAW','Vendor')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    cast(AccountNumber as string) as account_number,
    Name as name,
    CreditRating as credit_rating, -- 	1 = Superior, 2 = Excellent, 3 = Above average, 4 = Average, 5 = Below average
    PreferredVendorStatus as prefer_status -- 	0 = Vendor no longer used. 1 = Vendor is actively used.
from src
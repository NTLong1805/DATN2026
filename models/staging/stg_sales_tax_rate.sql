with src as(
    select *
    from {{source('DATN_RAW','SalesTaxRate')}}
)
select
    cast(_SalesTaxRateID as string) as _id,
    cast(StateProvinceID as string) as state_province_id,
    TaxType as tax_type, 	-- 1 = Tax applied to retail transactions, 2 = Tax applied to wholesale transactions, 3 = Tax applied to all sales (retail and wholesale) transactions.
    Name as tax_name,
    TaxRate as tax_rate,
    Cast(ModifiedDate as timestamp) as _ts
from src
    with src as(
        select *
        from {{source('DATN_RAW','Store')}}
    )
    select
        cast(_BusinessEntityID as string) as _id,
        cast(SalesPersonID as string) as salesperson_id,
        Name as store_name,
        safe_cast(regexp_extract(Demographics, r'<AnnualSales>(\d+)</AnnualSales>') as int64) as annual_sales,
        safe_cast(regexp_extract(Demographics, r'<AnnualRevenue>(\d+)</AnnualRevenue>') as int64) as annual_revenue,
        regexp_extract(Demographics, r'<BankName>([^<]+)</BankName>') as bank_name,
        regexp_extract(Demographics, r'<BusinessType>([^<]+)</BusinessType>') as business_type,
        safe_cast(regexp_extract(Demographics, r'<YearOpened>(\d+)</YearOpened>') as int64) as year_opened,
        regexp_extract(Demographics,r'<Specialty>(.*?)</Specialty>') as specialty,
        regexp_extract(Demographics,r'<SquareFeet>(\d+)</SquareFeet>') as square,
        regexp_extract(Demographics,r'<Brands>(.*?)</Brands>') as brand,
        regexp_extract(Demographics,r'<Internet>(.*?)</Internet>') as internet,
        safe_cast(regexp_extract(Demographics,r'<NumberEmployees>(\d+)</NumberEmployees>') as int64) as number_employee,
        cast(ModifiedDate as timestamp) as _ts
    from src

with src as(
    select *
    from {{source('DATN_RAW','person')}}
)
select
    cast(_BusinessEntityID as string) as _id,
    Title as title,
    Suffix as suffix,
    LastName as last_name,
    MiddleName as middle_name,
    FirstName as first_name,
    NameStyle as name_style,
    PersonType as person_type,
    safe_cast(regexp_extract(Demographics, r'<TotalPurchaseYTD>(.*?)</TotalPurchaseYTD>') as numeric) as purchase_amount_ytd,
    safe_cast(replace(regexp_extract(Demographics, r'<DateFirstPurchase>(.*?)</DateFirstPurchase>'), 'Z', '') as date) as date_first_purchase,
    safe_cast(replace(regexp_extract(Demographics, r'<BirthDate>(.*?)</BirthDate>'), 'Z', '') as date) as birth_date,
    safe_cast(regexp_extract(Demographics, r'<MaritalStatus>(.*?)</MaritalStatus>') as string) as marital_status,
    safe_cast(regexp_extract(Demographics, r'<YearlyIncome>(.*?)</YearlyIncome>') as string) as yearly_income,
    safe_cast(regexp_extract(Demographics, r'<Gender>(.*?)</Gender>') as string) as sex,
    safe_cast(regexp_extract(Demographics, r'<TotalChildren>(.*?)</TotalChildren>') as int64) as total_children,
    safe_cast(regexp_extract(Demographics, r'<NumberChildrenAtHome>(.*?)</NumberChildrenAtHome>') as int64) as total_children_at_home,
    safe_cast(regexp_extract(Demographics, r'<Education>(.*?)</Education>') as string) as education,
    safe_cast(regexp_extract(Demographics, r'<Occupation>(.*?)</Occupation>') as string) as occupation,
    safe_cast(regexp_extract(Demographics, r'<HomeOwnerFlag>(.*?)</HomeOwnerFlag>') as int64) as is_homeowner,
    safe_cast(regexp_extract(Demographics, r'<NumberCarsOwned>(.*?)</NumberCarsOwned>') as int64) as car_owned,
    safe_cast(regexp_extract(Demographics, r'<CommuteDistance>(.*?)</CommuteDistance>') as string) as commute_distance,
    EmailPromotion as email_promotion,
    AdditionalContactInfo as additional_contact_info
from src


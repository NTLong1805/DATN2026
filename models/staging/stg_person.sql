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
    Demographics as demographics,
    EmailPromotion as email_promotion,
    AdditionnalContactInfo as additional_contact_info
from src
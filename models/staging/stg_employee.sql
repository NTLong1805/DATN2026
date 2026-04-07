with src as(
    select *
    from {{source('DATN_RAW','Employee')}}
),
final as(
    select
        Cast(_BusinessEntityID as string) as _id,
        cast(NationalIDNumber as string) as national_id,
        cast(LoginID as string) as login_id,
        Path as path_node,
        OrganizationLevel as organization_level,
        JobTitle as job_title,
        cast(BirthDate as timestamp) as birth_date,
        MaritalStatus as martial_status,
        Gender as sex,
        cast(HireDate as timestamp) as hire_date,
        SalariedFlag as is_offical,
        VacationHours as holiday_hour,
        SickLeaveHours as absent_hour,
        CurrentFlag as status, -- 1 active 0 inactive
        cast(ModifiedDate as timestamp) as _ts
    from src
)
select *
from final
where _id is not null and path_node is not null
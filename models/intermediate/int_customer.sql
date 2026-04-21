with cte as(
    select
        c._id,
        c.person_id,
        c.account_number,
        p.person_type,
        concat(coalesce(p.last_name,' '),' ',coalesce(p.middle_name,' '),' ',coalesce(p.first_name,' ')) as full_name,
        pp.phone_number,
        pnt.phone_number_type,
        ea.email_address,
        bea.address_id,
        p.email_promotion,
        p.purchase_amount_ytd,
        p.date_first_purchase,
        p.birth_date,
        p.marital_status,
        p.yearly_income,
        p.sex,
        p.total_children,
        p.total_children_at_home,
        p.education,
        p.occupation,
        p.is_homeowner,
        p.car_owned,
        p.commute_distance
    from {{ref('stg_customer')}} as c
    left join {{ref('stg_person')}} as p
        on p._id = c.person_id
    left join {{ref('stg_person_phone')}} pp
        on pp.person_id = p._id
    left join {{ref('stg_email_address')}} ea
        on ea.person_id = p._id
    left join {{ref('stg_phone_number_type')}} pnt
        on pnt._id = pp.phone_number_type_id
    left join {{ref('stg_business_entity_address')}} as bea
        on bea._id = p._id
)
select *
from cte

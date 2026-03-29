select
    a._id,
    a.postal_code,
    a.main_address,
    a.city,
    a.state_province_id,
    att.Name as address_type,
    sp.state_province_name,
    st.territory_name as region,
    cr.country,
    st.continent,
    a._ts
from {{ref('stg_address')}} a
left join {{ref('stg_state_province')}} sp
    on a.state_province_id = sp._id
left join {{ref('stg_business_entity_address')}} bea
    on a._id = bea.address_id
left join {{ref('stg_addresstype')}} att
    on att._id = bea.address_type_id
left join {{ref('stg_country_region')}} cr
    on cr._id = sp.country_region_code
left join {{ref('stg_sales_territory')}} st
    on st._id = territory_id



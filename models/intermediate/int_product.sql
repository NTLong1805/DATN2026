with avg_rating as(
    select
        pr.product_id,
        avg(pr.rating) as avg_rating
    from {{ref('stg_product')}} p
    left join {{ref('stg_product_review')}} pr
        on p._id = pr.product_id
    group by 1
)
select
    p._id,
    p.product_number,
    p.product_model_id,
    p.product_subcategory_id,
    pc.category_name,
    ps.subcategory_name,
    p.product_name,
    pd.description,
    p.size,
    p.class,
    p.color,
    p.style,
    p.weight,
    p.is_manufactured,
    p_scd2.price,
    c_scd2.standard_cost,
    p.type,
    p.start_date,
    p.end_date,
    p.end_manufacted_date,
    p.min_inventory_quantity,
    p.reorder_point_quantity,
    p.status,
    p.manufacture_days,
    p_scd2.start_date as price_start_date,
    p_scd2.end_date as price_end_date,
    c_scd2.start_date as cost_start_date,
    c_scd2.end_date as cost_end_date,
    round(pr.avg_rating,2) as avg_rating
from {{ref('stg_product')}} as p
left join {{ref('stg_product_subcategory')}} as ps
    on p.product_subcategory_id = ps._id
left join {{ref('stg_product_category')}} as pc
    on ps.product_category_id = pc._id
left join {{ref('int_product_cost_scd2')}} as c_scd2
    on p._id = c_scd2.product_id and c_scd2.is_current = true
left join {{ref('int_product_price_scd2')}} as p_scd2
    on p._id = p_scd2.product_id and p_scd2.is_current = true
left join {{ref('stg_product_description_culture')}} as pdc
    on p.product_model_id = pdc._id and pdc.culture_id = 'en'
left join {{ref('stg_product_description')}} as pd
    on pd._id = pdc.description_id
left join avg_rating as pr
    on pr.product_id = p._id

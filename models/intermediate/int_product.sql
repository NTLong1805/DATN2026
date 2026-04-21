select
    p._id,
    p.product_number,
    p.product_model_id,
    p.product_subcategory_id,
    p.product_name,
    pdc.description,
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
    pc.category_name,
    ps.subcategory_name
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
    on p.product_model_id = pdc._id and
left join {{ref('stg_product_description')}} as pd
    on pd._id = pdc.description_id

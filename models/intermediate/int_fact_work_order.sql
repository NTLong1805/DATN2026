select
    wor._id,
    wor.product_id,
    wor.operation_sequence,
    wor.location_id,
    wor.schedule_start_date,
    wor.schedule_end_date,
    wor.real_start_date,
    wor.real_end_date,
    wor.time_spent,
    wor.planned_cost,
    wor.real_cost,
    wo.order_quantity,
    wo.scrapped_quantity,
    wo.stocked_quantity,
    sr.scrap_name
from {{ref('stg_work_order')}} wo
left join {{ref('stg_work_order_routing')}} as wor
    on wor._id = wo._id
left join {{ref('stg_scrap_reason')}} sr
    on sr._id = wo.scrap_reason_id

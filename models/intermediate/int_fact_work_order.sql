-- Có thể sử dụng để phân tích trường hợp xử lí null
-- vì work_order và work_order_rounting không trùng số dòng
-- => sẽ bị null ở productID => records vô giá trị
-- => fill thêm giá trị khác sẽ ảnh hưởng đến việc phân tích => Loại
with cte as(
    select
    wo._id,
    wor.product_id,
    wor.operation_sequence,
    wor.location_id,
    wo.start_date,
    wo.end_date,
    wo.due_date,
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
where wor.product_id is not null
)
select *
from cte

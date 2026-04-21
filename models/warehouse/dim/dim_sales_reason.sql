select s.order_id,s.reason_id,sr.reason_name,sr.reason_type
from {{ref('int_sales_order_header_sales_reason')}} s
left join {{ref('int_sales_reason')}}  sr
    on sr._id = s.reason_id

with final as(
    select
        soh.order_id,
        soh.reason_id,
        sr.reason_name,
        sr.reason_type
    from {{ref('int_sales_reason')}} sr
    left join {{ref('int_sales_order_header_sales_reason')}} soh
        on sr._id = soh.reason_id
)
select *
from final
-- 1 dòng là 1 store - 1 product mà store đó cung cấp
with final as(
    select
        v._id as vendor_id,
        v.account_number,
        v.name as vendor_name,
        v.credit_rating,
        v.prefer_status,
        pv.product_id,
        pv.average_leadtime,
        pv.last_receipt_cost,
        pv.max_order_quantity,
        pv.min_order_quantity
    from {{ref('dim_vendor')}} as v
    left join {{ref('dim_product_vendor')}} as pv
        on pv._id = v._id
)
select *
from final
where product_id is not null



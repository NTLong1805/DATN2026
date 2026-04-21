with final as(
    select so._id,
           sop.product_id,
           so.offer_type,
           so.description,
           so.category,
           so.discount_pct,
           so.min_quantity,
           coalesce(so.max_quantity,9999999) as max_quantity,
           so.start_date,
           so.end_date
    from {{ref('dim_special_offer')}} so
    left join {{ref('dim_special_offer_product')}} sop
        on sop.specialoffer_id = so._id
)
select *
from final
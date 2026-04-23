-- Sales History Sales Person Territory Efficent
-- Thêm 1 cột tính toán từ sales nữa để so sánh được KPI

with cte as(
    select
        _id,
        quota_date,
        sales_quota,
        lead(quota_date) over(
            partition by _id
            order by quota_date
        ) as next_quota_date,
        lead(sales_quota) over(
            partition by _id
            order by quota_date
        ) as next_sales_quota
    from {{ref('stg_sales_person_quota_history')}}
)
select
    st._id as territory_id,
    sp._id as salesperson_id,
    st.territory_name,
    st.country_region_code,
    case
        when cte.next_quota_date is not null
        then cte.next_sales_quota
        else sp.sales_quota_amount
    end as sales_quota_history,
    sp.bonus_amount,
    sp.commission_amount,
    cte.quota_date as start_date,
    case
        when cte.next_quota_date > sth.end_date
             or cte.next_quota_date is null
        then sth.end_date
        else cte.next_quota_date
    end as end_date
from cte
left join {{ref('stg_sales_person')}} sp
on sp._id = cte._id
left join {{ref('stg_sales_territory')}} st
on st._id = sp.territory_id
left join {{ref('stg_sales_territory_history')}} sth
on sth.salesperson_id = sp._id and st._id = sth.territory_id and cte.quota_date between sth.start_date and sth.end_date
order by cte._id,cte.quota_date


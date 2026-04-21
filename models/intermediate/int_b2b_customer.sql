-- 1 dòng là 1 KH doanh nghiệp của cty và 1 nhân viên chuyên phụ trách cho KH đó
select
    _id as customer_id,
    salesperson_id,
    store_name as b2b_name,
    annual_sales,
    annual_revenue,
    bank_name,
    case
        when business_type = 'BM' then 'Business Small'
        when business_type = 'BS' then 'Business Standard'
        when business_type = 'OS' then 'Big Corp'
    end as business_type,
    year_opened,
    specialty,
    square,
    brand,
    internet,
    number_employee
from {{ref('stg_store')}}
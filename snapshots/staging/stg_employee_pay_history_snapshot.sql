{%snapshot stg_employee_pay_history_snapshot%}
{{
    config(
        target_schema = 'snapshot',
        unique_key = '_id',
        strategy  = 'timestamp',
        updated_at = 'rate_change_date'
    )
}}
    with final as(
        select
            _id,
            salary,
            pay_frequency,
            rate_change_date,
            rank() over(partition by _id order by rate_change_date desc) as rn
        from {{ref('stg_employee_pay_history')}}
    )
    select *
    from final
    where rn = 1
{%endsnapshot%}
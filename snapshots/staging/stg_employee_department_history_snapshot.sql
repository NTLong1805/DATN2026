{% snapshot stg_employee_department_history_snapshot %}
{{
    config(
        target_schema = 'snapshot',
        unique_key = '_id',
        strategy = 'timestamp',
        updated_at = 'start_date'
    )
}}
    select
        _id,
        department_id,
        shift_id,
        start_date,
        end_date
    from {{ref('stg_employee_department_history')}}

{%endsnapshot%}
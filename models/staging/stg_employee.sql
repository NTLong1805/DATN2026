with src as(
    select *
    from {{source('DATN_RAW','Employee')}}
)
select
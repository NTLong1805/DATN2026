{{
    config(
        materialized = "table"
    )
}}
with date_spine as(
    {{dbt_utils.date_spine(
        datepart = "day",
        start_date = "cast('2008-01-01' as date)",
        end_date = "cast('2015-01-01' as date)"
    )}}
)
SELECT
    date_day AS date,

    -- key
    FORMAT_DATE('%Y%m%d', date_day) AS date_key,

    -- basic
    EXTRACT(YEAR FROM date_day) AS year,
    EXTRACT(MONTH FROM date_day) AS month,
    EXTRACT(DAY FROM date_day) AS day,

    -- hierarchy
    EXTRACT(QUARTER FROM date_day) AS quarter,
    EXTRACT(WEEK FROM date_day) AS week,

    -- name
    FORMAT_DATE('%A', date_day) AS day_name,
    FORMAT_DATE('%B', date_day) AS month_name,

    -- useful fields
    CASE
        WHEN EXTRACT(DAYOFWEEK FROM date_day) IN (1,7)
        THEN TRUE ELSE FALSE
    END AS is_weekend,

    CASE
        WHEN date_day = DATE_TRUNC(date_day, MONTH)
        THEN TRUE ELSE FALSE
    END AS is_month_start,

    CASE
        WHEN date_day = LAST_DAY(date_day)
        THEN TRUE ELSE FALSE
    END AS is_month_end

FROM date_spine
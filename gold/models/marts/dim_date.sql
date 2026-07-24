-- dim_date: standard calendar date spine, generated rather than sourced (no upstream
-- system "owns" the calendar). Covers a wide fixed range so it never needs regenerating
-- as new sales data arrives.

with spine as (
    select
        cast(unnest(generate_series(
            date '2024-01-01',
            date '2027-12-31',
            interval 1 day
        )) as date) as calendar_date
)

select
    cast(strftime(calendar_date, '%Y%m%d') as integer) as date_sk,
    calendar_date,
    extract(year from calendar_date)                   as year,
    extract(quarter from calendar_date)                as quarter,
    extract(month from calendar_date)                  as month,
    extract(dow from calendar_date)                     as day_of_week,
    (extract(dow from calendar_date) in (0, 6))          as is_weekend
from spine

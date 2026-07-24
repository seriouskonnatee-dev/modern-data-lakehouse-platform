-- mart_daily_store_sales: BI-ready aggregate -- one row per (store, date), the grain a
-- "daily revenue by store" dashboard would query directly without further aggregation.

select
    st.store_id,
    st.store_name,
    st.region,
    d.calendar_date,
    d.is_weekend,
    count(distinct f.event_id)          as n_transactions,
    sum(f.quantity)                     as total_units_sold,
    sum(f.line_amount)                  as total_revenue,
    round(avg(f.line_amount), 2)        as avg_line_amount
from {{ ref('fct_sales') }} f
join {{ ref('dim_store') }} st on f.store_sk = st.store_sk
join {{ ref('dim_date') }} d on f.date_sk = d.date_sk
group by 1, 2, 3, 4, 5

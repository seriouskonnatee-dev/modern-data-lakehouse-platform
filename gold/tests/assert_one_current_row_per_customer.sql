-- Singular dbt test: the core SCD2 invariant for dim_customer_scd2 is that every
-- customer_id has EXACTLY one row with is_current = true. dbt tests fail if this
-- query returns any rows, so we select the violations (customers with != 1 current row).

select
    customer_id,
    count(*) as n_current_rows
from {{ ref('dim_customer_scd2') }}
where is_current
group by customer_id
having count(*) != 1

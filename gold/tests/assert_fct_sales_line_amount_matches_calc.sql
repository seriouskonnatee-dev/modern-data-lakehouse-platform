-- Singular dbt test: line_amount should always equal quantity * unit_price -
-- discount_amount (rounded to cents). Catches upstream Silver enrichment regressions.

select
    sale_line_sk,
    line_amount,
    round(quantity * unit_price - discount_amount, 2) as expected_line_amount
from {{ ref('fct_sales') }}
where abs(line_amount - round(quantity * unit_price - discount_amount, 2)) > 0.01

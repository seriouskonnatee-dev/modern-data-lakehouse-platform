-- dim_product: slowly changing reference data (Type 1 -- products don't need history
-- tracking for this portfolio's business questions, unlike customer loyalty tier).

select
    row_number() over (order by product_id) as product_sk,
    product_id,
    product_name,
    category,
    subcategory,
    unit_cost,
    list_price
from {{ ref('ref_products') }}

-- dim_store: slowly changing reference data (Type 1).

select
    row_number() over (order by store_id) as store_sk,
    store_id,
    store_name,
    region,
    store_type
from {{ ref('ref_stores') }}

-- dim_customer_scd2: Slowly Changing Dimension Type 2 over customer loyalty_tier and
-- home_store_id. See docs/adr/0004-scd2-merge-strategy.md for why this is implemented
-- as a full-history window-function recompute rather than a stateful incremental merge.

with snapshots as (

    select distinct
        customer_id,
        loyalty_tier,
        home_store_id,
        email_domain,
        snapshot_ts
    from {{ ref('stg_customer_profiles') }}

),

with_prior as (

    select
        *,
        lag(loyalty_tier) over (partition by customer_id order by snapshot_ts)   as prior_tier,
        lag(home_store_id) over (partition by customer_id order by snapshot_ts)  as prior_store
    from snapshots

),

change_points as (

    -- Keep the first snapshot per customer, and any snapshot where a tracked
    -- attribute actually changed vs. the immediately preceding snapshot.
    select
        customer_id,
        loyalty_tier,
        home_store_id,
        email_domain,
        snapshot_ts
    from with_prior
    where prior_tier is null
       or loyalty_tier is distinct from prior_tier
       or home_store_id is distinct from prior_store

),

with_effective_dates as (

    select
        customer_id,
        loyalty_tier,
        home_store_id,
        email_domain,
        cast(snapshot_ts as date) as effective_start_date,
        cast(
            lead(snapshot_ts) over (partition by customer_id order by snapshot_ts)
            as date
        ) as effective_end_date_exclusive
    from change_points

),

final as (

    select
        row_number() over (order by customer_id, effective_start_date) as customer_sk,
        customer_id,
        loyalty_tier,
        home_store_id,
        email_domain,
        effective_start_date,
        case
            when effective_end_date_exclusive is null then null
            else effective_end_date_exclusive - interval 1 day
        end as effective_end_date,
        (effective_end_date_exclusive is null) as is_current,
        current_timestamp as dbt_loaded_at
    from with_effective_dates

)

select
    customer_sk,
    customer_id,
    loyalty_tier,
    home_store_id,
    email_domain,
    cast(effective_start_date as date) as effective_start_date,
    cast(effective_end_date as date)   as effective_end_date,
    is_current,
    dbt_loaded_at
from final

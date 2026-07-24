-- mart_customer_ltv: one row per *current* customer (joins to the is_current dim_customer
-- row), summarizing order history. A simplified LTV proxy -- see docs/design.md §3.6 and
-- the caveats section of customer-segmentation-clv for the more rigorous CLV treatment
-- elsewhere in this portfolio; this mart is intentionally the lightweight BI-facing view.

with customer_orders as (

    select
        f.customer_sk,
        count(distinct f.event_id)  as total_orders,
        sum(f.line_amount)          as total_revenue,
        min(cast(f.event_ts as date)) as first_purchase_date,
        max(cast(f.event_ts as date)) as last_purchase_date
    from {{ ref('fct_sales') }} f
    where f.customer_sk is not null
    group by 1

),

current_customers as (

    select * from {{ ref('dim_customer_scd2') }}
    where is_current

),

final as (

    select
        cc.customer_sk,
        cc.customer_id,
        cc.loyalty_tier,
        coalesce(co.total_orders, 0)                             as total_orders,
        coalesce(co.total_revenue, 0.0)                          as total_revenue,
        round(coalesce(co.total_revenue, 0) /
              nullif(coalesce(co.total_orders, 0), 0), 2)         as avg_order_value,
        co.first_purchase_date,
        co.last_purchase_date,
        -- simplified LTV proxy: observed revenue-to-date x a tier-based multiplier
        -- standing in for expected future purchases. A real LTV model belongs in
        -- customer-segmentation-clv (this portfolio's dedicated CLV project); this mart
        -- exists to show a BI-consumable "good enough for a dashboard filter" figure.
        round(
            coalesce(co.total_revenue, 0) * case cc.loyalty_tier
                when 'platinum' then 2.5
                when 'gold'     then 1.8
                when 'silver'   then 1.3
                else 1.0
            end,
            2
        ) as estimated_ltv
    from current_customers cc
    left join customer_orders co on cc.customer_sk = co.customer_sk

)

select * from final

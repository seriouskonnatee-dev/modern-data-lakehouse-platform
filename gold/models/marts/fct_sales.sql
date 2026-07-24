-- fct_sales: sale-line grain fact table. customer_sk is resolved to the dim_customer
-- row that was actually effective on event_ts (SCD2-correct "as of" attribution) --
-- see docs/adr/0004-scd2-merge-strategy.md.

with sales as (

    select * from {{ ref('stg_sales_events') }}

),

customer_history as (

    select * from {{ ref('dim_customer_scd2') }}

),

sales_with_customer_sk as (

    select
        s.*,
        c.customer_sk
    from sales s
    left join customer_history c
        on s.customer_id = c.customer_id
        and cast(s.event_ts as date) >= c.effective_start_date
        and (
            c.effective_end_date is null
            or cast(s.event_ts as date) <= c.effective_end_date
        )

),

final as (

    select
        {{ generate_surrogate_key(['sales_with_customer_sk.event_id']) }} as sale_line_sk,
        sales_with_customer_sk.event_id,
        sales_with_customer_sk.customer_sk,
        p.product_sk,
        st.store_sk,
        cast(strftime(sales_with_customer_sk.event_ts, '%Y%m%d') as integer) as date_sk,
        sales_with_customer_sk.quantity,
        sales_with_customer_sk.unit_price,
        sales_with_customer_sk.discount_amount,
        sales_with_customer_sk.line_amount,
        sales_with_customer_sk.event_ts,
        sales_with_customer_sk.silver_loaded_at
    from sales_with_customer_sk
    left join {{ ref('dim_product') }} p on sales_with_customer_sk.product_id = p.product_id
    left join {{ ref('dim_store') }} st on sales_with_customer_sk.store_id = st.store_id

)

select * from final

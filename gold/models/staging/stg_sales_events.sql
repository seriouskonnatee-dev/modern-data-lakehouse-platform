-- Staging pass over Silver sales_events_clean: light renaming/typing only, no business
-- logic yet (that lives in marts/). Reads Silver Parquet directly via DuckDB's
-- read_parquet, which is the "no live warehouse needed" story documented in ADR 0003.

with source as (

    select *
    from read_parquet(
        '{{ var("silver_lake_path") }}/sales_events_clean/**/*.parquet',
        hive_partitioning = true
    )

),

renamed as (

    select
        event_id,
        event_type,
        customer_id,
        product_id,
        store_id,
        cast(quantity as integer)        as quantity,
        cast(unit_price as double)       as unit_price,
        cast(discount_amount as double)  as discount_amount,
        cast(line_amount as double)      as line_amount,
        cast(event_ts as timestamp)      as event_ts,
        cast(silver_loaded_at as timestamp) as silver_loaded_at,
        is_late_arrival,
        event_date

    from source
    where event_type = 'sale'  -- fct_sales models completed sales; refunds/voids are
                                -- out of scope for this portfolio's fact table but the
                                -- field is preserved here for anyone extending this

)

select * from renamed

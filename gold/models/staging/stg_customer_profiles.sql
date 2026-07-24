-- Staging pass over Silver customer_profiles_clean.

with source as (

    select *
    from read_parquet('{{ var("silver_lake_path") }}/customer_profiles_clean/**/*.parquet')

),

renamed as (

    select
        customer_id,
        full_name,
        email,
        split_part(email, '@', 2) as email_domain,
        loyalty_tier,
        home_store_id,
        cast(snapshot_ts as timestamp)      as snapshot_ts,
        cast(silver_loaded_at as timestamp) as silver_loaded_at

    from source

)

select * from renamed

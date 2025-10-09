{{ config(
    materialized='table'
) }}

with sponsors_source as (
    select * from {{ source('staging', 'sponsors') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

sponsors as (
    select
        sponsor_key,
        sponsor_name,
        sponsor_class,
        etl_created_at

    from sponsors_source
)

select * from sponsors
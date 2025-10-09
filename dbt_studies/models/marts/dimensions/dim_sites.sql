{{ config(
    materialized='table'
) }}


with sites_source as (
    select * from {{ source('staging', 'sites') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

with sites as (
    select
        site_key,
        facility_name,
        city,
        state as site_state,
        zip,
        country,
        etl_created_at

    from sites_source
)

select * from sites
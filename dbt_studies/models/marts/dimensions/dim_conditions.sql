{{ config(
    materialized='table'
) }}

with conditions_source as (
    select * from {{ source('staging', 'conditions') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

conditions as (
    select
        condition_key,
        condition_name,
        etl_created_at

    from conditions_source
)

select * from conditions
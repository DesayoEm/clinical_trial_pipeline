{{ config(
    materialized='table'
) }}

with conditions_source as (
    select * from {{ source('staging', 'conditions') }}
),

conditions as (
    select
        condition_key,
        condition_name,
        etl_created_at

    from conditions_source
)

select * from conditions
{{ config(
    materialized='table',
    schema='analytics'
) }}

with source as (
    select * from {{ source('clinical_trials', 'conditions') }}
),

with conditions as (
    select
        condition_key,
        condition_name,

    from source
)

select * from conditions
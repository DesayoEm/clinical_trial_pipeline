{{ config(
    materialized='table',
    schema='analytics'
) }}

with source as (
    select * from {{ source('clinical_trials', 'sites') }}
),

with sites as (
    select
        site_key,
        facility_name,
        city,
        state as site_state,
        zip,
        country
    from source
)

select * from sites
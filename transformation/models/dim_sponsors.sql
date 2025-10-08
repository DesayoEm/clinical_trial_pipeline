{{ config(
    materialized='table',
    schema='analytics'
) }}

with source as (
    select * from {{ source('clinical_trials', 'sponsors') }}
),

with sponsors as (
    select
        sponsor_key,
        sponsor_name,
        sponsor_class

    from source
)

select * from sponsors
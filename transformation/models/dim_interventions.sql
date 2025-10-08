{{ config(
    materialized='table',
    schema='analytics'
) }}

with source as (
    select * from {{ source('clinical_trials', 'interventions') }}
),

with interventions as (
    select
        intervention_key,
        intervention_type,
        intervention_name,
        intervention_description

    from source
)

select * from interventions
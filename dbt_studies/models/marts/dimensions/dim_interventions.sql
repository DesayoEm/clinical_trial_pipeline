{{ config(
    materialized='table'
) }}


with interventions_source as (
    select * from {{ source('staging', 'interventions') }}
),

interventions as (
    select
        intervention_key,
        intervention_type,
        intervention_name,
        intervention_description,
        etl_created_at

    from interventions_source
)

select * from interventions
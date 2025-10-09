{{ config(
    materialized='table'
) }}


with interventions_source as (
    select * from {{ source('staging', 'interventions') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
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
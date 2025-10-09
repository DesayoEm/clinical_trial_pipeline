{{
    config(
        materialized='incremental',
        unique_key='study_intervention_key',
        on_schema_change='append_new_columns'
    )
}}

with study_interventions_source as (
    select * from {{ source('staging', 'study_interventions') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

final as (
    select
        study_intervention_key,
        study_key,
        intervention_key,
        etl_created_at
    from study_interventions_source
)

select * from final
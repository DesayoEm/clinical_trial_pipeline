{{
    config(
        materialized='incremental',
        unique_key='study_sponsor_key',
        on_schema_change='append_new_columns'
    )
}}

with study_sponsors_source as (
    select * from {{ source('staging', 'study_sponsors') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

final as (
    select
        study_sponsor_key,
        study_key,
        sponsor_key,
        is_lead,
        is_collaborator,
        etl_created_at
    from study_sponsors_source
)

select * from final
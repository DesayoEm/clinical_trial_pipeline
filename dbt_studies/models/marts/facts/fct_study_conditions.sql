{{
    config(
        materialized='incremental',
        unique_key='study_condition_key',
        on_schema_change='append_new_columns'
    )
}}

with study_conditions_source as (
    select * from {{ source('staging', 'study_conditions') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

final as (
    select
        study_condition_key,
        study_key,
        condition_key,
        etl_created_at
    from study_conditions_source
)

select * from final
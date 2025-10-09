{{
    config(
        materialized='incremental',
        unique_key='study_site_key',
        on_schema_change='append_new_columns'
    )
}}

with study_sites_source as (
    select * from {{ source('staging', 'study_sites') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

final as (
    select
        study_site_key,
        study_key,
        site_key,
        etl_created_at
    from study_sites_source
)

select * from final
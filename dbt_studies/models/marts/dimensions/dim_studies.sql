{{ config(
    materialized='table'
) }}


with studies_source as (
    select * from {{ source('staging', 'studies') }}
    {% if is_incremental() %}
    where etl_created_at > (select max(etl_created_at) from {{ this }})
    {% endif %}
),

transformed as (
    select
        study_key,
        nct_id,
        brief_title,
        official_title,
        acronym,
        brief_summary,
        detailed_description,
        

        overall_status,
        why_stopped,
        case
            when overall_status in ('RECRUITING', 'ACTIVE_NOT_RECRUITING', 'ENROLLING_BY_INVITATION') 
            then true else false
        end as is_active,

        start_date,
        start_date_type,
        completion_date,
        completion_date_type,
        primary_completion_date,
        primary_completion_date_type,
        source_last_updated_date,
        source_last_updated_date_type,

        case when start_date ~ '^\d{4}-\d{2}$'
            then (start_date || '-01')::date
        end as start_date_parsed,
        
        case when completion_date ~ '^\d{4}-\d{2}$'
            then (completion_date || '-01')::date
        end as completion_date_parsed,


        study_type,
        allocation,
        intervention_model,
        primary_purpose,
        masking,
        masking_description,
        patient_registry,
        target_duration,

        enrollment_count,
        case 
            when enrollment_count >= 1000 then 'Large'
            when enrollment_count >= 100 then 'Medium'
            when enrollment_count > 0 then 'Small'
            else 'Unknown'
        end as enrollment_category,

        healthy_volunteers,
        sex,
        minimum_age_years,
        maximum_age_years,
        case
            when minimum_age_years >= 65 then 'Seniors'
            when minimum_age_years >= 18 then 'Adults'
            when maximum_age_years < 18 then 'Pediatric'
            else 'All Ages'
        end as age_group,

        has_expanded_access,
        has_dmc,

        current_timestamp as dbt_created_at,
        current_timestamp as dbt_updated_at,
        etl_created_at
        
    from studies_source
)

select * from transformed
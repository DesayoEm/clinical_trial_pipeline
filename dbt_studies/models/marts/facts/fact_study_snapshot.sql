{{
    config(
        materialized='incremental',
        unique_key='snapshot_key',
        on_schema_change='append_new_columns'
    )
}}

with snapshot_date as (
    select
        cast(to_char(current_date, 'YYYYMMDD') as integer) as snapshot_date_key,
        current_date as snapshot_date
),

studies as (
    select
        study_key,
        nct_id,
        overall_status,
        enrollment_count,
        is_active
    from {{ ref('dim_studies') }}
),

study_sponsors as (
    select
        study_key,
        count(*) as num_sponsors
    from {{ ref('fact_study_sponsor') }}
    group by study_key
),

study_conditions as (
    select
        study_key,
        count(*) as num_conditions
    from {{ ref('fact_study_condition') }}
    group by study_key
),

study_interventions as (
    select
        study_key,
        count(*) as num_interventions
    from {{ ref('fact_study_intervention') }}
    group by study_key
),

study_sites as (
    select
        study_key,
        count(*) as num_sites
    from {{ ref('fact_study_site') }}
    group by study_key
),

final as (
    select
        md5(
            coalesce(cast(sd.snapshot_date as text), '') || '|' ||
            coalesce(cast(s.study_key as text), '')
        ) as snapshot_key,
        sd.snapshot_date_key,
        s.study_key,
        s.overall_status,
        s.enrollment_count,
        coalesce(sp.num_sponsors, 0) as num_sponsors,
        coalesce(sc.num_conditions, 0) as num_conditions,
        coalesce(si.num_interventions, 0) as num_interventions,
        coalesce(ss.num_sites, 0) as num_sites,
        s.is_active,
        current_timestamp as created_at
    from snapshot_date sd
    cross join studies s
    left join study_sponsors sp on sp.study_key = s.study_key
    left join study_conditions sc on sc.study_key = s.study_key
    left join study_interventions si on si.study_key = s.study_key
    left join study_sites ss on ss.study_key = s.study_key

    {% if is_incremental() %}
    where sd.snapshot_date_key > (select max(snapshot_date_key) from {{ this }})
    {% endif %}
)

select * from final
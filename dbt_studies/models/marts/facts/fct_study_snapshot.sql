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
        count(*) as num_sites,
        count(case when site_status in ('RECRUITING', 'ACTIVE') then 1 end) as num_active_sites
    from {{ ref('fact_study_site') }}
    group by study_key
),

final as (
    select
        {{ dbt_utils.generate_surrogate_key(['sd.snapshot_date_key', 's.study_key']) }} as snapshot_key,
        sd.snapshot_date_key,
        s.study_key,
        s.overall_status,
        s.enrollment_count,
        coalesce(sp.num_sponsors, 0) as num_sponsors,
        coalesce(sc.num_conditions, 0) as num_conditions,
        coalesce(si.num_interventions, 0) as num_interventions,
        coalesce(ss.num_sites, 0) as num_sites,
        coalesce(ss.num_active_sites, 0) as num_active_sites,
        s.is_active,
        current_timestamp() as created_at
    from snapshot_date sd
    cross join studies s
    left join study_sponsors sp on s.study_key = sp.study_key
    left join study_conditions sc on s.study_key = sc.study_key
    left join study_interventions si on s.study_key = si.study_key
    left join study_sites ss on s.study_key = ss.study_key

    {% if is_incremental() %}
    where sd.snapshot_date_key > (select max(snapshot_date_key) from {{ this }})
    {% endif %}
)

select * from final
{{ config(
    tags=["mnpi_exception", "product"],
    materialized = "incremental",
    unique_key = "event_pk",
    on_schema_change = "sync_all_columns"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_event_all', 'prep_event_all'),
    ('prep_user', 'prep_user')
    ])
}},

fct_events AS (

  SELECT
    prep_event_all.event_pk, 
    prep_event_all.event_id,
    prep_event_all.event_name,
    prep_event_all.ultimate_parent_namespace_id,
    prep_event_all.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.
    {{ get_keyed_nulls('prep_user.dim_user_sk') }} AS dim_user_sk,
    prep_event_all.parent_type,
    prep_event_all.parent_id,
    prep_event_all.dim_project_id,
    prep_event_all.event_created_at,
    prep_event_all.namespace_created_at,
    prep_event_all.plan_was_paid_at_event_timestamp,
    prep_event_all.plan_id_at_event_timestamp,
    prep_event_all.plan_name_at_event_timestamp,
    prep_event_all.days_since_user_creation_at_event_date,
    prep_event_all.days_since_namespace_creation_at_event_date,
    prep_event_all.days_since_project_creation_at_event_date,
    CAST(prep_event_all.event_created_at AS DATE) AS event_date,
    IFF(prep_event_all.dim_user_id IS NULL, TRUE, FALSE) AS is_null_user
  FROM prep_event_all
  LEFT JOIN prep_user
    --dim_user_id is the natural_key and will be renamed to user_id in a subsequent MR on prep_event.
    ON prep_event_all.dim_user_id = prep_user.user_id

  {% if is_incremental() %}

   WHERE event_created_at > (SELECT DATEADD(DAY, -30 , max(event_created_at)) FROM {{ this }})

  {% endif %}


),

gitlab_dotcom_fact AS (

  SELECT
    --Primary Key
    fct_events.event_pk,

    --Natural Key
    fct_events.event_id,

    --Foreign Keys
    dim_date.date_id AS dim_event_date_id,
    fct_events.ultimate_parent_namespace_id AS dim_ultimate_parent_namespace_id,
    fct_events.dim_project_id,
    fct_events.dim_user_sk,
    fct_events.dim_user_id,--dim_user_id is the current foreign key, and is a natural_key, and will be updated to user_id in a future MR.

    --Time attributes
    fct_events.event_created_at,
    fct_events.event_date,
    fct_events.namespace_created_at,

    --Degenerate Dimensions (No stand-alone, promoted dimension table)
    fct_events.is_null_user,
    fct_events.parent_id,
    fct_events.parent_type,
    fct_events.event_name,
    fct_events.plan_id_at_event_timestamp,
    fct_events.plan_name_at_event_timestamp,
    fct_events.plan_was_paid_at_event_timestamp,
    fct_events.days_since_user_creation_at_event_date,
    fct_events.days_since_namespace_creation_at_event_date,
    fct_events.days_since_project_creation_at_event_date,
    'GITLAB_DOTCOM' AS data_source
  FROM fct_events
  LEFT JOIN dim_date
    ON fct_events.event_date = dim_date.date_day

)

{{ dbt_audit(
    cte_ref="gitlab_dotcom_fact",
    created_by="@icooper-acp",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2023-08-10"
) }}

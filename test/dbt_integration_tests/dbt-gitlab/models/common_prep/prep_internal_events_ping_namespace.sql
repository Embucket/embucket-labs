{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('internal_events_ping_namespace', 'internal_events_ping_namespace'),
    ('dim_date', 'dim_date')
]) }}

, transformed AS (

    SELECT
      saas_usage_ping_gitlab_dotcom_namespace_id,
      namespace_ultimate_parent_id                                    AS dim_namespace_id,
      ping_name                                                       AS ping_name, --potentially renamed
      ping_date, --currently wrong date input in the airflow run
      TO_DATE(_uploaded_at)                                           AS run_date,
      counter_value
    FROM internal_events_ping_namespace
    WHERE error = 'Success'

), joined AS (

    SELECT
      saas_usage_ping_gitlab_dotcom_namespace_id,
      dim_namespace_id,
      ping_name,
      ping_date,
      counter_value
    FROM transformed
    INNER JOIN dim_date ON ping_date = date_day

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@rbacovic",
    updated_by="@rbacovic",
    created_date="2024-10-10",
    updated_date="2024-10-10"
) }}

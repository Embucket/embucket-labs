{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('saas_usage_ping_namespace', 'saas_usage_ping_namespace'),
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
    FROM saas_usage_ping_namespace
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
    created_by="@mpeychet_",
    updated_by="@mdrussell",
    created_date="2021-03-22",
    updated_date="2023-07-27"
) }}

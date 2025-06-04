{%- set first_ = dbt_utils.get_query_results_as_dict("SELECT MIN(date_actual) AS date FROM " ~ ref('dim_date')) -%} --> First date in dim_date, used to back-fill the initial default settings for CI minutes and storage limits.
{%- set first_ci_minute_limit = 2000 -%}
{%- set first_repository_storage_limit = 10737418240 -%} --> 1 GiB, in bytes.
{%- set first_ci_minute_limit_change_date = "2020-10-01" -%}  --> Date that the default limit for CI minutes was updated from 2000 to 400.
{%- set first_app_settings_snapshot_id = "442ab0695cfd8a3fa7ffbddb959903ad" -%}  --> This will be used to identify the row we need to back-fill to {{first_ci_minute_limit_change_date}} since the snapshot table was created several month after the default setting was updated.

{{ simple_cte([
    ('app_settings', 'gitlab_dotcom_application_settings_snapshots_base'),
    ('dates', 'dim_date')
]) }}

,  application_settings_historical AS (

    SELECT
      application_settings_snapshot_id,
      IFF(application_settings_snapshot_id = '{{  first_app_settings_snapshot_id  }}',
          '{{  first_ci_minute_limit_change_date  }}', valid_from)      AS valid_from,
      IFNULL(valid_to, CURRENT_TIMESTAMP)                               AS valid_to,
      application_settings_id,
      shared_runners_minutes,
      repository_size_limit
    FROM app_settings
    
    UNION ALL
    
    SELECT
      MD5('-1')                                                         AS application_settings_snapshot_id,
      '{{  first_.DATE[0]  }}'                                          AS valid_from,
      DATEADD('ms', -1, '{{  first_ci_minute_limit_change_date  }}')    AS valid_to,
      -1                                                                AS application_settings_id,
      {{  first_ci_minute_limit  }}                                     AS shared_runners_minutes,
      {{  first_repository_storage_limit  }}                            AS repository_size_limit

), application_settings_snapshot_monthly AS (
  
    SELECT
      DATE_TRUNC('month', dates.date_actual)                            AS snapshot_month,
      application_settings_historical.application_settings_id,
      application_settings_historical.shared_runners_minutes,
      application_settings_historical.repository_size_limit
    FROM application_settings_historical
    INNER JOIN dates
      ON dates.date_actual BETWEEN application_settings_historical.valid_from
                               AND application_settings_historical.valid_to
    QUALIFY ROW_NUMBER() OVER(
      PARTITION BY snapshot_month
      ORDER BY valid_from DESC
      ) = 1
  
), keyed AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['snapshot_month',
                                  'application_settings_id']) }}        AS primary_key,
      *
    FROM application_settings_snapshot_monthly

)

{{ dbt_audit(
    cte_ref="keyed",
    created_by="@ischweickartDD",
    updated_by="@ischweickartDD",
    created_date="2021-03-30",
    updated_date="2021-03-30"
) }}



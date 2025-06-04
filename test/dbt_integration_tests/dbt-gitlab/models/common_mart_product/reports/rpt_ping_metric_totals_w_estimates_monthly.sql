{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('mart_ping_instance_metric_monthly', 'mart_ping_instance_metric_monthly'),
    ('mart_pct', 'rpt_ping_metric_estimate_factors_monthly')
    ])

}}

-- Fact data from mart_ping_instance_metric_monthly, bringing in only last ping of months which are valid

, fact AS (

    SELECT
      metrics_path                      AS metrics_path,
      ping_created_date_month           AS ping_created_date_month,
      ping_delivery_type                AS ping_delivery_type,
      ping_deployment_type              AS ping_deployment_type,
      ping_edition                      AS ping_edition,
      ping_product_tier                 AS ping_product_tier,
      ping_edition_product_tier         AS ping_edition_product_tier,
      stage_name                        AS stage_name,
      section_name                      AS section_name,
      group_name                        AS group_name,
      is_smau                           AS is_smau,
      is_gmau                           AS is_gmau,
      is_paid_gmau                      AS is_paid_gmau,
      is_umau                           AS is_umau,
      SUM(monthly_metric_value)         AS recorded_usage
    FROM mart_ping_instance_metric_monthly
    WHERE time_frame IN ('28d', 'all')
      AND (
        is_internal = FALSE
        OR dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7' --GitLab.com production installation
      )
    {{ dbt_utils.group_by(n=14)}}

--Join in adoption percentages to determine what to estimate for self managed

), sm_joined_counts_w_percentage AS (

  SELECT
      fact.*,
      mart_pct.reporting_count          AS reporting_count,
      mart_pct.not_reporting_count      AS not_reporting_count,
      mart_pct.percent_reporting        AS percent_reporting,
      mart_pct.estimation_grain         AS estimation_grain
    FROM fact
      INNER JOIN mart_pct
    ON fact.ping_created_date_month = mart_pct.ping_created_date_month
      AND fact.metrics_path = mart_pct.metrics_path
      AND fact.ping_edition = mart_pct.ping_edition
      AND fact.ping_deployment_type = mart_pct.ping_deployment_type
  WHERE fact.ping_deployment_type IN ('Self-Managed', 'Dedicated')

-- No need to join in SaaS, it is what it is (all reported and accurate)

), saas_joined_counts_w_percentage AS (

  SELECT
      fact.*,
      1                                         AS reporting_count,
      0                                         AS not_reporting_count,
      1                                         AS percent_reporting,
      'GitLab.com'                              AS estimation_grain
    FROM fact
  WHERE ping_deployment_type = 'GitLab.com'

-- Union SaaS and Self Managed tables

), joined_counts_w_percentage AS (

  SELECT * FROM saas_joined_counts_w_percentage

  UNION ALL

  SELECT * FROM sm_joined_counts_w_percentage

--Format output

), final AS (

SELECT
    {{ dbt_utils.generate_surrogate_key(['ping_created_date_month', 'metrics_path', 'ping_edition', 'estimation_grain', 'ping_edition_product_tier', 'ping_deployment_type']) }}   AS ping_metric_totals_w_estimates_monthly_id,
    -- identifiers
    metrics_path                                                                                                                                                        AS metrics_path,
    ping_created_date_month                                                                                                                                             AS ping_created_date_month,
    ping_delivery_type                                                                                                                                                  AS ping_delivery_type,
    ping_deployment_type                                                                                                                                                AS ping_deployment_type,
    -- ping attributes
    ping_edition                                                                                                                                                        AS ping_edition,
    ping_product_tier                                                                                                                                                   AS ping_product_tier,
    ping_edition_product_tier                                                                                                                                           AS ping_edition_product_tier,
    -- metric attributes
    stage_name                                                                                                                                                          AS stage_name,
    section_name                                                                                                                                                        AS section_name,
    group_name                                                                                                                                                          AS group_name,
    is_smau                                                                                                                                                             AS is_smau,
    is_gmau                                                                                                                                                             AS is_gmau,
    is_paid_gmau                                                                                                                                                        AS is_paid_gmau,
    is_umau                                                                                                                                                             AS is_umau,
    -- fct info
    reporting_count                                                                                                                                                     AS reporting_count,
    not_reporting_count                                                                                                                                                 AS not_reporting_count,
    percent_reporting                                                                                                                                                   AS percent_reporting,
    estimation_grain                                                                                                                                                    AS estimation_grain,
    ROUND({{ usage_estimation('recorded_usage', 'percent_reporting') }})                                                                                                AS total_usage_with_estimate,
    ROUND(total_usage_with_estimate - recorded_usage)                                                                                                                   AS estimated_usage,
    recorded_usage                                                                                                                                                      AS recorded_usage
 FROM joined_counts_w_percentage
 WHERE ping_created_date_month < DATE_TRUNC('month', CURRENT_DATE)

)

 {{ dbt_audit(
     cte_ref="final",
     created_by="@icooper-acp",
     updated_by="@jpeguero",
     created_date="2022-04-20",
     updated_date="2023-06-26"
 ) }}

{{ config({    
    "materialized":"view",
    "tags":["product", "mnpi_exception"]
}) }}

{{ simple_cte([
    ('rpt_ping_metric_totals_w_estimates_monthly', 'rpt_ping_metric_totals_w_estimates_monthly'),
    ('rpt_event_xmau_metric_monthly', 'rpt_event_xmau_metric_monthly')
    ])
}}, service_ping_with_estimate AS (

  SELECT
    ping_created_date_month AS reporting_month,
    section_name,
    stage_name,
    group_name,
    estimation_grain,
    metrics_path,
    ping_product_tier,
    ping_delivery_type,
    ping_deployment_type,
    ping_edition,
    total_usage_with_estimate,
    recorded_usage,
    estimated_usage,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau
FROM rpt_ping_metric_totals_w_estimates_monthly
ORDER BY reporting_month DESC

), gitlab_dotcom_usage AS (

  SELECT
    event_calendar_month AS reporting_month,
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_umau,
    event_name_array,
    user_group,
    user_count
  FROM rpt_event_xmau_metric_monthly
  ORDER BY reporting_month DESC

), service_ping_gitlab_dotcom_unioned AS (

  SELECT
    reporting_month,
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metrics_path AS metric,
    IFF(ping_deployment_type = 'GitLab.com', 'GitLab.com', ping_product_tier) AS product_tier,
    CASE
        WHEN ping_deployment_type = 'Self-Managed' THEN 'Recorded Self-Managed'
        WHEN ping_deployment_type = 'Dedicated' THEN 'Recorded Dedicated'
        ELSE ping_deployment_type
    END AS breakdown,
    ping_delivery_type AS delivery,
    ping_deployment_type AS deployment,
    IFF(ping_deployment_type = 'GitLab.com', 'GitLab.com', ping_edition) AS edition,
    SUM(recorded_usage) AS metric_value
  FROM service_ping_with_estimate
  {{ dbt_utils.group_by(n=15) }}
  
  UNION ALL
  
  SELECT --estimated SM totals
    reporting_month,
    section_name,
    stage_name,
    group_name,
      is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metrics_path AS metric,
    ping_product_tier AS product_tier,
    CASE
      WHEN ping_deployment_type = 'Self-Managed' THEN 'Estimated Self-Managed Uplift'
      WHEN ping_deployment_type = 'Dedicated' THEN 'Estimated Dedicated Uplift'
      ELSE ping_deployment_type
    END AS breakdown,
    ping_delivery_type AS delivery,
    ping_deployment_type AS deployment,
    IFF(ping_deployment_type = 'GitLab.com', 'GitLab.com', ping_edition) AS edition,
    ROUND(SUM(estimated_usage)) AS metric_value
  FROM service_ping_with_estimate
  WHERE ping_deployment_type IN ('Self-Managed','Dedicated')
  {{ dbt_utils.group_by(n=15) }}
  
  UNION ALL
  
  SELECT --get paid SaaS xMAU (if applicable)
    reporting_month,
    section_name,
    stage_name,
    group_name,
      is_smau,
    is_gmau,
    is_gmau AS is_paid_gmau,
    is_umau,
    'Paid GitLab.com' AS estimation_grain,
    ARRAY_TO_STRING(event_name_array, ',') AS metric,
    'Paid GitLab.com' AS product_tier,
    'Paid GitLab.com' AS breakdown,
    'Paid SaaS' AS delivery,
    'Paid GitLab.com' AS deployment,
    'Paid GitLab.com' AS edition,
    SUM(user_count) AS metric_value
  FROM gitlab_dotcom_usage
  WHERE  user_group = 'paid' 
    AND reporting_month >= (SELECT MIN(reporting_month) 
FROM service_ping_with_estimate) 
  {{ dbt_utils.group_by(n=15) }}

), results AS (

SELECT 
  {{ dbt_utils.generate_surrogate_key(['service_ping_gitlab_dotcom_unioned.reporting_month', 'service_ping_gitlab_dotcom_unioned.metric', 'service_ping_gitlab_dotcom_unioned.product_tier','service_ping_gitlab_dotcom_unioned.breakdown']) }}
                                               AS event_namespace_monthly_pk,  
  *,
  LAG(metric_value,12) OVER (PARTITION BY
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metric,
    product_tier,
    breakdown,
    delivery,
    deployment,
    edition
 ORDER BY reporting_month ASC ) AS previous_year_metric_value,
  LAG(metric_value,1) OVER (PARTITION BY
    section_name,
    stage_name,
    group_name,
    is_smau,
    is_gmau,
    is_paid_gmau,
    is_umau,
    estimation_grain,
    metric,
    product_tier,
    breakdown,
    delivery,
    deployment,
    edition
ORDER BY reporting_month ASC ) AS previous_month_metric_value,
    MAX(IFF(is_umau = TRUE, metric_value,NULL)) OVER (PARTITION BY reporting_month,    
 estimation_grain,
    product_tier,
    breakdown,
    delivery,
    deployment,
    edition) AS umau_value
FROM service_ping_gitlab_dotcom_unioned

)


{{ dbt_audit(
    cte_ref="results",
    created_by="@dpeterson",
    updated_by="@dpeterson",
    created_date="2023-07-20",
    updated_date="2023-07-27"
) }}

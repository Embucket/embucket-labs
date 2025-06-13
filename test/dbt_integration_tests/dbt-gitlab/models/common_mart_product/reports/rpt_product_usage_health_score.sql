{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('paid_user_metrics', 'mart_product_usage_paid_user_metrics_monthly'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges')
])}}

, joined AS (

    SELECT
        paid_user_metrics.snapshot_month,
        paid_user_metrics.primary_key,
        dim_crm_account.crm_account_name,
        paid_user_metrics.dim_crm_account_id,
        dim_crm_account.customer_since_date,
        DATEDIFF(MONTH, dim_crm_account.customer_since_date, paid_user_metrics.snapshot_month)                          AS account_age_months,
        DATEDIFF(MONTH, paid_user_metrics.subscription_start_date, paid_user_metrics.snapshot_month)                    AS subscription_age_months,
        COALESCE(paid_user_metrics.installation_creation_date, paid_user_metrics.namespace_creation_date)               AS combined_instance_creation_date,
        DATEDIFF(DAY, combined_instance_creation_date, paid_user_metrics.ping_created_at)                               AS instance_age_days,
        DATEDIFF(MONTH, combined_instance_creation_date, paid_user_metrics.ping_created_at)                             AS instance_age_months,
        paid_user_metrics.subscription_start_date,
        paid_user_metrics.subscription_name,
        paid_user_metrics.dim_subscription_id_original,
        paid_user_metrics.dim_subscription_id,
        IFF(mart_arr_all.product_tier_name ILIKE '%Ultimate%', 1, 0)                                                        AS ultimate_subscription_flag,
        IFF(mart_arr_all.product_rate_plan_name ILIKE '%OSS Program%' AND SUM(mart_arr_all.arr) OVER (PARTITION BY mart_arr_all.dim_subscription_id_original, mart_arr_all.arr_month, mart_arr_all.product_delivery_type) = 0, TRUE, FALSE)                                                        AS is_oss_program,
        paid_user_metrics.delivery_type,
        paid_user_metrics.deployment_type,
        paid_user_metrics.instance_type,
        paid_user_metrics.included_in_health_measures_str,
        paid_user_metrics.uuid,
        paid_user_metrics.hostname,
        paid_user_metrics.dim_namespace_id,
        paid_user_metrics.dim_installation_id,
        IFF(paid_user_metrics.dim_namespace_id IS NULL, paid_user_metrics.dim_installation_id, paid_user_metrics.dim_namespace_id)      AS instance_identifier,
        COALESCE(paid_user_metrics.hostname, paid_user_metrics.dim_namespace_id)                                                        AS hostname_or_namespace_id,
        paid_user_metrics.ping_created_at,
        paid_user_metrics.cleaned_version,

-- license utilization metrics --
        paid_user_metrics.license_utilization,
        paid_user_metrics.billable_user_count,
        paid_user_metrics.license_user_count,
        paid_user_metrics.license_user_count_source,
        paid_user_metrics.duo_pro_license_user_count,
        paid_user_metrics.duo_pro_billable_user_count,
        paid_user_metrics.duo_pro_billable_user_count/paid_user_metrics.duo_pro_license_user_count                                                                        AS duo_pro_license_utilization,
        paid_user_metrics.duo_enterprise_license_user_count,
        paid_user_metrics.duo_enterprise_billable_user_count,
        paid_user_metrics.duo_enterprise_billable_user_count/paid_user_metrics.duo_enterprise_license_user_count                                                          AS duo_enterprise_license_utilization,
        CASE WHEN paid_user_metrics.duo_pro_billable_user_count IS NULL
              AND paid_user_metrics.duo_enterprise_billable_user_count IS NULL THEN NULL
             ELSE ZEROIFNULL(paid_user_metrics.duo_pro_billable_user_count)+ZEROIFNULL(paid_user_metrics.duo_enterprise_billable_user_count) END                          AS duo_total_billable_user_count,
        CASE WHEN paid_user_metrics.duo_pro_license_user_count IS NULL
              AND paid_user_metrics.duo_enterprise_license_user_count IS NULL THEN NULL
             ELSE ZEROIFNULL(paid_user_metrics.duo_pro_license_user_count)+ZEROIFNULL(paid_user_metrics.duo_enterprise_license_user_count) END                            AS duo_total_license_user_count,
        duo_total_billable_user_count/duo_total_license_user_count                                                                                                        AS duo_total_license_utilization,
        (CASE WHEN account_age_months < 3 THEN NULL
        WHEN account_age_months >= 3 and account_age_months < 7 THEN (CASE WHEN paid_user_metrics.license_utilization <= .10 THEN 25
                                                                            WHEN paid_user_metrics.license_utilization > .10 and paid_user_metrics.license_utilization <= .50 THEN 63
                                                                            WHEN paid_user_metrics.license_utilization > .50 THEN 88
                                                                            else NULL end)
        WHEN account_age_months >= 7 and account_age_months < 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .50 THEN 25
                                                                            WHEN paid_user_metrics.license_utilization > .50 and paid_user_metrics.license_utilization <= .75 THEN 63
                                                                            WHEN paid_user_metrics.license_utilization > .75 THEN 88
                                                                            else NULL end)
        WHEN account_age_months >= 10 THEN (CASE WHEN paid_user_metrics.license_utilization <= .75 THEN 25
                                                WHEN paid_user_metrics.license_utilization > .75 THEN 88
                                                else NULL end)
        end) AS license_utilization_score,
        CASE WHEN license_utilization_score IS NULL THEN NULL
            WHEN license_utilization_score = 25 THEN 'Red'
            WHEN license_utilization_score = 63 THEN 'Yellow'
            WHEN license_utilization_score = 88 THEN 'Green' end AS license_utilization_color,

-- user engagement metrics --
        paid_user_metrics.last_activity_28_days_user,
        CASE WHEN paid_user_metrics.deployment_type = 'GitLab.com' THEN NULL
            WHEN paid_user_metrics.deployment_type IN ('Self-Managed', 'GitLab.com') THEN div0(paid_user_metrics.last_activity_28_days_user, paid_user_metrics.billable_user_count) end AS user_engagement,
        CASE WHEN user_engagement IS NULL THEN NULL
            WHEN user_engagement < .50 THEN 25
            WHEN user_engagement >= .50 and user_engagement < .80 THEN 63
            WHEN user_engagement >= .80 THEN 88 end AS user_engagement_score,
        CASE WHEN user_engagement_score IS NULL THEN NULL
            WHEN user_engagement_score = 25 THEN 'Red'
            WHEN user_engagement_score = 63 THEN 'Yellow'
            WHEN user_engagement_score = 88 THEN 'Green' end AS user_engagement_color,

-- scm metrics --
        CASE WHEN instance_identifier IS NULL THEN NULL else paid_user_metrics.action_monthly_active_users_project_repo_28_days_user end AS action_monthly_active_users_project_repo_28_days_user_clean,
        div0(action_monthly_active_users_project_repo_28_days_user_clean, paid_user_metrics.billable_user_count) AS git_operation_utilization,
        CASE WHEN git_operation_utilization IS NULL THEN NULL
            WHEN git_operation_utilization < .25 THEN 25
            WHEN git_operation_utilization >= .25 and git_operation_utilization < .50 THEN 63
            WHEN git_operation_utilization >= .50 THEN 88 end AS scm_score,
        CASE WHEN scm_score IS NULL THEN NULL
            WHEN scm_score = 25 THEN 'Red'
            WHEN scm_score = 63 THEN 'Yellow'
            WHEN scm_score = 88 THEN 'Green' end AS scm_color,

-- ci metrics --
        paid_user_metrics.ci_pipelines_28_days_user,
        div0(paid_user_metrics.ci_pipelines_28_days_user, paid_user_metrics.billable_user_count) AS ci_pipeline_utilization,
        paid_user_metrics.ci_builds_28_days_user,
        paid_user_metrics.ci_builds_all_time_user,
        paid_user_metrics.ci_builds_all_time_event,
        paid_user_metrics.ci_runners_all_time_event,
        paid_user_metrics.ci_builds_28_days_event,
        CASE WHEN ci_pipeline_utilization > 0.333 THEN 88
                  WHEN ci_pipeline_utilization > 0.1 AND ci_pipeline_utilization <= 0.333 THEN 63
                  WHEN ci_pipeline_utilization <= 0.1 THEN 25
                  ELSE NULL END AS ci_pipeline_utilization_score,
        CASE WHEN ci_pipeline_utilization_score IS NULL THEN NULL
            WHEN ci_pipeline_utilization_score = 25 THEN 'Red'
            WHEN ci_pipeline_utilization_score = 63 THEN 'Yellow'
            WHEN ci_pipeline_utilization_score = 88 THEN 'Green' end               AS ci_pipeline_utilization_color,
        ci_pipeline_utilization_score AS ci_score,
        CASE WHEN ci_score IS NULL THEN NULL
            WHEN ci_score = 25 THEN 'Red'
            WHEN ci_score = 63 THEN 'Yellow'
            WHEN ci_score = 88 THEN 'Green' end AS ci_color,

-- ci lighthouse metrics --
        paid_user_metrics.ci_builds_28_days_event_post_16_9,
        div0(paid_user_metrics.ci_builds_28_days_event_post_16_9, paid_user_metrics.billable_user_count) AS ci_builds_per_billable_user,
        CASE WHEN ci_builds_per_billable_user IS NULL THEN NULL
            WHEN ci_builds_per_billable_user > 30 THEN 88
            WHEN ci_builds_per_billable_user > 1 AND ci_builds_per_billable_user <= 30 THEN 63
            WHEN ci_builds_per_billable_user <= 1 THEN 25
            ELSE NULL END AS ci_builds_per_billable_user_score,
        CASE WHEN ci_builds_per_billable_user_score IS NULL THEN NULL
            WHEN ci_builds_per_billable_user_score = 25 THEN 'Red'
            WHEN ci_builds_per_billable_user_score = 63 THEN 'Yellow'
            WHEN ci_builds_per_billable_user_score = 88 THEN 'Green' END AS ci_builds_per_billable_user_color,
        ci_builds_per_billable_user_score AS ci_lighthouse_score,
        ci_builds_per_billable_user_color AS ci_lighthouse_color,

-- cd metrics --
        paid_user_metrics.deployments_28_days_user,
        paid_user_metrics.deployments_28_days_event,
        paid_user_metrics.successful_deployments_28_days_event,
        paid_user_metrics.failed_deployments_28_days_event,
        (paid_user_metrics.successful_deployments_28_days_event + paid_user_metrics.failed_deployments_28_days_event) AS completed_deployments_l28d,
        paid_user_metrics.projects_all_time_event,
        paid_user_metrics.environments_all_time_event,
        div0(paid_user_metrics.deployments_28_days_user, paid_user_metrics.billable_user_count) AS deployments_utilization,
        CASE WHEN deployments_utilization IS NULL THEN NULL
            WHEN deployments_utilization < .05 THEN 25
            WHEN deployments_utilization >= .05 and deployments_utilization <= .12 THEN 63
            WHEN deployments_utilization > .12 THEN 88 end AS deployments_utilization_score,
        CASE WHEN deployments_utilization_score IS NULL THEN NULL
            WHEN deployments_utilization_score = 25 THEN 'Red'
            WHEN deployments_utilization_score = 63 THEN 'Yellow'
            WHEN deployments_utilization_score = 88 THEN 'Green' end AS deployments_utilization_color,
        div0(paid_user_metrics.deployments_28_days_event,paid_user_metrics.billable_user_count) AS deployments_per_user_l28d,
        CASE WHEN deployments_per_user_l28d IS NULL THEN NULL
            WHEN deployments_per_user_l28d < 2 THEN 25
            WHEN deployments_per_user_l28d >= 2 and deployments_per_user_l28d <= 7 THEN 63
            WHEN deployments_per_user_l28d > 7 THEN 88 end AS deployments_per_user_l28d_score,
        CASE WHEN deployments_per_user_l28d_score IS NULL THEN NULL
            WHEN deployments_per_user_l28d_score = 25 THEN 'Red'
            WHEN deployments_per_user_l28d_score = 63 THEN 'Yellow'
            WHEN deployments_per_user_l28d_score = 88 THEN 'Green' end AS deployments_per_user_l28d_color,
        div0(successful_deployments_28_days_event, completed_deployments_l28d) AS successful_deployments_pct,
        CASE WHEN successful_deployments_pct IS NULL THEN NULL
            WHEN successful_deployments_pct < .25 THEN 25
            WHEN successful_deployments_pct >= .25 and successful_deployments_pct <= .80 THEN 63
            WHEN successful_deployments_pct > .80 THEN 88 end AS successful_deployments_pct_score,
        CASE WHEN successful_deployments_pct_score IS NULL THEN NULL
            WHEN successful_deployments_pct_score = 25 THEN 'Red'
            WHEN successful_deployments_pct_score = 63 THEN 'Yellow'
            WHEN successful_deployments_pct_score = 88 THEN 'Green' end AS successful_deployments_pct_color,
        IFF(deployments_utilization_score IS NOT NULL, 1, 0)
            + IFF(deployments_per_user_l28d_score IS NOT NULL, 1, 0)
            + IFF(successful_deployments_pct_score IS NOT NULL, 1, 0) AS cd_measure_count,
        IFF(cd_measure_count = 0, NULL, div0((zeroifnull(deployments_utilization_score)
          + zeroifnull(deployments_per_user_l28d_score)
          + zeroifnull(successful_deployments_pct_score)), cd_measure_count)) AS cd_score,
        IFF(cd_measure_count = 0, NULL, (CASE WHEN cd_score <= 50 THEN 'Red'
                                                WHEN cd_score > 50 and cd_score <= 75 THEN 'Yellow'
                                                WHEN cd_score > 75 THEN 'Green' end)) AS cd_color,

-- security metrics --
        paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user,
        paid_user_metrics.CI_INTERNAL_PIPELINES_28_DAYS_EVENT,
        paid_user_metrics.SECRET_DETECTION_SCANS_28_DAYS_EVENT,
        paid_user_metrics.DEPENDENCY_SCANNING_SCANS_28_DAYS_EVENT,
        paid_user_metrics.CONTAINER_SCANNING_SCANS_28_DAYS_EVENT,
        paid_user_metrics.DAST_SCANS_28_DAYS_EVENT,
        paid_user_metrics.SAST_SCANS_28_DAYS_EVENT,
        paid_user_metrics.COVERAGE_FUZZING_SCANS_28_DAYS_EVENT,
        paid_user_metrics.API_FUZZING_SCANS_28_DAYS_EVENT,
        paid_user_metrics.SECRET_DETECTION_SCANS_28_DAYS_EVENT
            + paid_user_metrics.DEPENDENCY_SCANNING_SCANS_28_DAYS_EVENT
            + paid_user_metrics.CONTAINER_SCANNING_SCANS_28_DAYS_EVENT
            + paid_user_metrics.DAST_SCANS_28_DAYS_EVENT
            + paid_user_metrics.SAST_SCANS_28_DAYS_EVENT
            + paid_user_metrics.COVERAGE_FUZZING_SCANS_28_DAYS_EVENT
            + paid_user_metrics.API_FUZZING_SCANS_28_DAYS_EVENT                                                                                 AS sum_of_all_scans_l28d,
        div0(paid_user_metrics.SECRET_DETECTION_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                      AS secret_detection_scan_percentage,
        div0(paid_user_metrics.DEPENDENCY_SCANNING_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                   AS dependency_scanning_scan_percentage,
        div0(paid_user_metrics.CONTAINER_SCANNING_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                    AS container_scanning_scan_percentage,
        div0(paid_user_metrics.DAST_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                                  AS DAST_scan_percentage,
        div0(paid_user_metrics.SAST_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                                  AS sast_scan_percentage,
        div0(paid_user_metrics.COVERAGE_FUZZING_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                      AS coverage_fuzzing_scan_percentage,
        div0(paid_user_metrics.API_FUZZING_SCANS_28_DAYS_EVENT,sum_of_all_scans_l28d)                                                           AS api_fuzzing_scan_percentage,
        div0(paid_user_metrics.user_unique_users_all_secure_scanners_28_days_user, paid_user_metrics.billable_user_count)                       AS secure_scanners_utilization,
        CASE WHEN secure_scanners_utilization IS NULL THEN NULL
            WHEN secure_scanners_utilization <= .05 THEN 25
            WHEN secure_scanners_utilization > .05 and secure_scanners_utilization < .20 THEN 63
            WHEN secure_scanners_utilization >= .20 THEN 88 end                                                                                 AS secure_scanners_utilization_score,
        CASE WHEN secure_scanners_utilization_score IS NULL THEN NULL
            WHEN secure_scanners_utilization_score = 25 THEN 'Red'
            WHEN secure_scanners_utilization_score = 63 THEN 'Yellow'
            WHEN secure_scanners_utilization_score = 88 THEN 'Green' end                                                                        AS secure_scanners_utilization_color,
        IFF(CI_INTERNAL_PIPELINES_28_DAYS_EVENT = 0, NULL, div0(sum_of_all_scans_l28d,paid_user_metrics.CI_INTERNAL_PIPELINES_28_DAYS_EVENT))   AS average_scans_per_pipeline,
        CASE WHEN average_scans_per_pipeline IS NULL THEN NULL
            WHEN average_scans_per_pipeline < 0.1 THEN 25
            WHEN average_scans_per_pipeline >= 0.1 AND average_scans_per_pipeline <= 0.5 THEN 63
            WHEN average_scans_per_pipeline > 0.5 THEN 88 end                                                                                   AS average_scans_per_pipeline_score,
        CASE WHEN average_scans_per_pipeline_score IS NULL THEN NULL
            WHEN average_scans_per_pipeline_score = 25 THEN 'Red'
            WHEN average_scans_per_pipeline_score = 63 THEN 'Yellow'
            WHEN average_scans_per_pipeline_score = 88 THEN 'Green' end                                                                         AS average_scans_per_pipeline_color,
        IFF(SECRET_DETECTION_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                     AS secret_detection_usage_flag,
        IFF(DEPENDENCY_SCANNING_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                  AS dependency_scanning_usage_flag,
        IFF(CONTAINER_SCANNING_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                   AS container_scanning_usage_flag,
        IFF(DAST_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                                 AS dast_usage_flag,
        IFF(SAST_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                                 AS sast_usage_flag,
        IFF(COVERAGE_FUZZING_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                     AS coverage_fuzzing_usage_flag,
        IFF(API_FUZZING_SCANS_28_DAYS_EVENT > 0, 1, 0)                                                                                          AS api_fuzzing_usage_flag,
        secret_detection_usage_flag
            + dependency_scanning_usage_flag
            + container_scanning_usage_flag
            + dast_usage_flag
            + sast_usage_flag
            + coverage_fuzzing_usage_flag
            + api_fuzzing_usage_flag                                                                                                            AS number_of_scanner_types,
        CASE WHEN number_of_scanner_types IS NULL THEN NULL
            WHEN number_of_scanner_types <= 1 THEN 25
            WHEN number_of_scanner_types >= 2 AND number_of_scanner_types <= 2 THEN 63
            WHEN number_of_scanner_types >=3 THEN 88 end                                                                                        AS number_of_scanner_types_score,
        CASE WHEN number_of_scanner_types_score IS NULL THEN NULL
            WHEN number_of_scanner_types_score = 25 THEN 'Red'
            WHEN number_of_scanner_types_score = 63 THEN 'Yellow'
            WHEN number_of_scanner_types_score = 88 THEN 'Green' end                                                                            AS number_of_scanner_types_color,
        IFF(secure_scanners_utilization_score IS NOT NULL, 1, 0)
            + IFF(average_scans_per_pipeline_score IS NOT NULL, 1, 0)
            + IFF(number_of_scanner_types_score IS NOT NULL, 1, 0)                                                                              AS security_measure_count,
        IFF(security_measure_count = 0, NULL, div0((zeroifnull(secure_scanners_utilization_score)
            + zeroifnull(average_scans_per_pipeline_score)
            + zeroifnull(number_of_scanner_types_score)), security_measure_count))                                                              AS security_score,
        IFF(security_measure_count = 0, NULL, CASE WHEN security_score <= 50 THEN 'Red'
                                                    WHEN security_score > 50 AND security_score <= 75 THEN 'Yellow'
                                                    WHEN security_score > 75 THEN 'Green' end)                                                  AS security_color,
        IFF(ultimate_subscription_flag = 1, security_score, NULL)                                                                               AS security_score_ultimate_only,
        IFF(ultimate_subscription_flag = 1, security_color, NULL)                                                                               AS security_color_ultimate_only,

-- overall product score --
        IFF(license_utilization_score IS NULL, 0, .30) AS license_utilization_weight,
        IFF(user_engagement_score IS NULL, 0, .10) AS user_engagement_weight,
        IFF(scm_score IS NULL, 0, .15) AS scm_weight,
        IFF(ci_score IS NULL, 0, .15) AS ci_weight,
        IFF(cd_score IS NULL, 0, .15) AS cd_weight,
        IFF(security_score_ultimate_only IS NULL, 0, .15) AS security_weight,
        license_utilization_weight + user_engagement_weight + scm_weight + ci_weight + cd_weight + security_weight AS remaining_weight,
        div0(license_utilization_weight,remaining_weight) AS adjusted_license_utilization_weight,
        div0(user_engagement_weight, remaining_weight) AS adjusted_user_engagement_weight,
        div0(scm_weight, remaining_weight) AS adjusted_scm_weight,
        div0(ci_weight, remaining_weight) AS adjusted_ci_weight,
        div0(cd_weight, remaining_weight) AS adjusted_cd_weight,
        div0(security_weight, remaining_weight) AS adjusted_security_weight,
        zeroifNULL(license_utilization_score) * adjusted_license_utilization_weight AS adjusted_license_utilization_score,
        zeroifNULL(user_engagement_score) * adjusted_user_engagement_weight AS adjusted_user_engagement_score,
        zeroifNULL(scm_score) * adjusted_scm_weight AS adjusted_scm_score,
        zeroifNULL(ci_score) * adjusted_ci_weight AS adjusted_ci_score,
        zeroifNULL(cd_score) * adjusted_cd_weight AS adjusted_cd_score,
        zeroifNULL(security_score_ultimate_only) * adjusted_security_weight AS adjusted_security_score,
        adjusted_license_utilization_score + adjusted_user_engagement_score + adjusted_scm_score + adjusted_ci_score + adjusted_cd_score + adjusted_security_score AS overall_product_score,
        CASE WHEN overall_product_score <= 50 THEN 'Red'
            WHEN overall_product_score > 50 and overall_product_score <= 75 THEN 'Yellow'
            WHEN overall_product_score > 75 THEN 'Green' end AS overall_product_color,
        iff(scm_color IS NULL, NULL, iff(scm_color = 'Green', 1, 0)) AS scm_adopted,
        iff(ci_color IS NULL, NULL, iff(ci_color = 'Green', 1, 0)) AS ci_adopted,
        iff(cd_color IS NULL, NULL, iff(cd_color = 'Green', 1, 0)) AS cd_adopted,
        iff(security_color_ultimate_only IS NULL, NULL, iff(security_color_ultimate_only = 'Green', 1, 0)) AS security_adopted,
        iff(scm_adopted IS NULL
             AND ci_adopted IS NULL
             AND cd_adopted IS NULL
             AND security_color_ultimate_only IS NULL, NULL, zeroifnull(scm_adopted) + zeroifnull(ci_adopted) + zeroifnull(cd_adopted) + zeroifnull(security_adopted)) AS total_use_cases_adopted,
        ARRAY_CONSTRUCT_COMPACT(
                IFF(scm_adopted = 1, 'SCM', NULL),
                IFF(ci_adopted = 1, 'CI', NULL),
                IFF(cd_adopted = 1, 'CD', NULL),
                IFF(security_adopted = 1, 'Security', NULL)
            ) AS adopted_use_case_names_array,
        ARRAY_TO_STRING(adopted_use_case_names_array, ', ') AS adopted_use_case_names_string

FROM paid_user_metrics
LEFT JOIN dim_crm_account
    ON paid_user_metrics.dim_crm_account_id = dim_crm_account.dim_crm_account_id
LEFT JOIN mart_arr_all
    ON paid_user_metrics.dim_subscription_id_original = mart_arr_all.dim_subscription_id_original
    AND paid_user_metrics.snapshot_month = mart_arr_all.arr_month
    AND paid_user_metrics.delivery_type = mart_arr_all.product_delivery_type
    AND mart_arr_all.product_tier_name NOT IN ('Storage','Not Applicable')
WHERE paid_user_metrics.license_user_count != 0
qualify row_number() OVER (PARTITION BY paid_user_metrics.snapshot_month, instance_identifier ORDER BY paid_user_metrics.ping_created_at DESC NULLs last) = 1

),

final as (

select
   *,
   CASE WHEN ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type, instance_type, is_oss_program ORDER BY billable_user_count desc nulls last, ping_created_at desc nulls last) = 1
              and instance_type = 'Production' and is_oss_program = false
             THEN True
             ELSE False
        END AS is_primary_instance_subscription,
   lag(ci_pipeline_utilization_color) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_color_previous_month,
   lag(ci_pipeline_utilization_color, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_color_previous_3_month,
	lag(ci_lighthouse_color) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_lighthouse_color_previous_month,
   lag(ci_lighthouse_color, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_lighthouse_color_previous_3_month,
   lag(ci_pipeline_utilization) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_pipeline_utilization_previous_month,
   lag(ci_pipeline_utilization, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_pipeline_utilization_previous_3_month,
   lag(ci_builds_per_billable_user) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_builds_per_billable_user_previous_month,
   lag(ci_builds_per_billable_user, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as ci_builds_per_billable_user_previous_3_month,
   lag(scm_color) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as scm_color_previous_month,
   lag(scm_color, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as scm_color_previous_3_month,
   lag(git_operation_utilization) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as git_operation_utilization_previous_month,
   lag(git_operation_utilization, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as git_operation_utilization_previous_3_month,
   lag(cd_color) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as cd_color_previous_month,
   lag(cd_color, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as cd_color_previous_3_month,
   lag(security_color_ultimate_only) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as security_color_previous_month,
   lag(security_color_ultimate_only, 3) over (partition by hostname_or_namespace_id
order by
   snapshot_month) as security_color_previous_3_month
from
   joined

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@snalamaru",
    updated_by="@bbutterfield",
    created_date="2023-12-10",
    updated_date="2025-03-05"
) }}

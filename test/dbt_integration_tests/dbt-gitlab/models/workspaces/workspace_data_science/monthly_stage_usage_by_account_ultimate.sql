{{ config(
     materialized = "table",
     tags=["mnpi_exception"]
) }}

WITH usage_ping AS (
    SELECT
        *,
        DATE_TRUNC('MONTH', ping_created_at) AS ping_created_at_month
    FROM {{ ref('prep_ping_instance') }}
    WHERE product_tier = 'Ultimate'
),

license_subscription_mapping AS (
    SELECT
        *
    FROM {{ ref('map_license_subscription_account') }}
),

dates AS (
    SELECT
        *
    FROM {{ ref('dim_date') }}
),

saas_usage_ping AS (
    SELECT
        *
FROM {{ ref('prep_saas_usage_ping_namespace') }}
),

namespace_subscription_bridge AS (
    SELECT
        *
    FROM {{ ref('bdg_namespace_order_subscription_monthly') }}
),

usage_ping_metrics AS (
    SELECT
        *
    FROM {{ ref('dim_ping_metric') }}
),

ultimate_namespaces AS (

    SELECT
      namespace_id,
      snapshot_month
    FROM {{ ref('gitlab_dotcom_namespace_lineage_historical_monthly') }} namespace_lineage_historical
    LEFT JOIN {{ ref('gitlab_dotcom_plans_source') }} plan_info
      ON namespace_lineage_historical.ultimate_parent_plan_id = plan_info.plan_id
    WHERE plan_name = 'ultimate'
        OR plan_name = 'gold'


),

sm_last_monthly_ping_per_account AS (
    SELECT
        COALESCE(sha256.dim_crm_account_id, md5.dim_crm_account_id)     AS dim_crm_account_id,
        COALESCE(sha256.dim_subscription_id, md5.dim_subscription_id)   AS dim_subscription_id,
        usage_ping.dim_instance_id                                      AS uuid,
        usage_ping.hostname,
        usage_ping.raw_usage_data_payload,
        CAST(usage_ping.ping_created_at_month AS DATE) AS snapshot_month
    FROM usage_ping
    LEFT JOIN license_subscription_mapping AS md5
      ON usage_ping.license_md5 = md5.license_md5
    LEFT JOIN license_subscription_mapping AS sha256
      ON usage_ping.license_sha256 = sha256.license_sha256
    WHERE (usage_ping.license_md5 IS NOT NULL OR usage_ping.license_sha256 IS NOT NULL)
        AND CAST(
            usage_ping.ping_created_at_month AS DATE
        ) < DATE_TRUNC('month', CURRENT_DATE)
  QUALIFY ROW_NUMBER () OVER (
    PARTITION BY
      sha256.dim_subscription_id,
      md5.dim_subscription_id,
      usage_ping.dim_instance_id,
      usage_ping.hostname,
      CAST(usage_ping.ping_created_at_month AS DATE)
    ORDER BY
      usage_ping.ping_created_at DESC
  ) = 1
),

saas_last_monthly_ping_per_account AS (
    SELECT
        namespace_subscription_bridge.dim_crm_account_id,
        namespace_subscription_bridge.dim_subscription_id,
        namespace_subscription_bridge.dim_namespace_id,
        namespace_subscription_bridge.snapshot_month,
        saas_usage_ping.ping_name AS metrics_path,
        saas_usage_ping.counter_value AS metrics_value
    FROM saas_usage_ping
    INNER JOIN dates ON saas_usage_ping.ping_date = dates.date_day
    INNER JOIN ultimate_namespaces
        ON ultimate_namespaces.namespace_id = saas_usage_ping.dim_namespace_id
        AND ultimate_namespaces.snapshot_month = dates.first_day_of_month
    INNER JOIN
        namespace_subscription_bridge ON
            saas_usage_ping.dim_namespace_id =
            namespace_subscription_bridge.dim_namespace_id
            AND dates.first_day_of_month =
            namespace_subscription_bridge.snapshot_month
            AND namespace_subscription_bridge.namespace_order_subscription_match_status = 'Paid All Matching'
    WHERE namespace_subscription_bridge.dim_crm_account_id IS NOT NULL
        AND namespace_subscription_bridge.snapshot_month < DATE_TRUNC(
            'month', CURRENT_DATE
        )
        AND metrics_path LIKE 'usage_activity_by_stage%'
        AND metrics_value > 0 -- Filter out non-instances
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY
      namespace_subscription_bridge.dim_crm_account_id,
      namespace_subscription_bridge.dim_namespace_id,
      namespace_subscription_bridge.snapshot_month,
      saas_usage_ping.ping_name
    ORDER BY
      saas_usage_ping.ping_date DESC
  ) = 1
),

flattened_metrics AS (
    SELECT
        dim_crm_account_id,
        dim_subscription_id,
        NULL AS dim_namespace_id,
        uuid,
        hostname,
        snapshot_month,
        "PATH" AS metrics_path,
        "VALUE" AS metrics_value
    FROM sm_last_monthly_ping_per_account,
        LATERAL FLATTEN(INPUT => raw_usage_data_payload, RECURSIVE => TRUE)
    WHERE metrics_path LIKE 'usage_activity_by_stage%'
        AND IS_REAL(metrics_value) = 1
        AND metrics_value > 0

    UNION ALL

    SELECT
        dim_crm_account_id,
        dim_subscription_id,
        dim_namespace_id,
        NULL AS uuid,
        NULL AS hostname,
        snapshot_month,
        metrics_path,
        metrics_value
    FROM saas_last_monthly_ping_per_account
)

SELECT
    flattened_metrics.dim_crm_account_id,
    flattened_metrics.snapshot_month,

    -- NUMBER OF FEATURES USED BY PRODUCT STAGE
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'plan'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_plan_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'plan'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_plan_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'create'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_create_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'create'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_create_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'verify'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_verify_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'verify'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_verify_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'package'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_package_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'package'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_package_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'monitor'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_monitor_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'monitor'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_monitor_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'secure'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_secure_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'secure'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_secure_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'govern'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_govern_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'govern'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_govern_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'deploy'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_deploy_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'deploy'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_deploy_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'foundations'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_foundations_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.stage_name = 'foundations'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS stage_foundations_28days_features,


    -- NUMBER OF FEATURES USED BY PRODUCT SECTION
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'dev'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_dev_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'dev'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_dev_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'sec'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_sec_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'sec'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_sec_28days_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'ci'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_ci_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'ci'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_ci_28days_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'cd'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_cd_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'cd'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_cd_28days_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'core_platform'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_core_platform_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'core_platform'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_core_platform_28days_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'analytics'
                AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_analytics_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                usage_ping_metrics.section_name = 'analytics'
                AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS section_analytics_28days_features,        


    -- NUMBER OF FEATURES USED BY PRODUCT TIER
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_free_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_free_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_premium_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'free'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_premium_28days_features,

    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'ultimate'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_ultimate_alltime_features,
    COUNT(
        DISTINCT CASE
            WHEN
                CONTAINS(
                    usage_ping_metrics.tier, 'ultimate'
                ) AND NOT CONTAINS(
                    usage_ping_metrics.tier, 'premium'
                ) AND usage_ping_metrics.time_frame = '28d'
                THEN flattened_metrics.metrics_path
        END
    ) AS tier_ultimate_28days_features,

    -- NUMBER OF TIMES FEATURES ARE USED BY STAGE
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'plan'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_plan_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'create'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_create_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'verify'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_verify_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'package'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_package_alltime_feature_sum,

    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'monitor'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_monitor_alltime_features_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'secure'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_secure_alltime_feature_sum,

    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'govern'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_govern_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'deploy'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_deploy_alltime_feature_sum,
    COALESCE(
        SUM(
            CASE
                WHEN
                    usage_ping_metrics.stage_name = 'foundations'
                    AND usage_ping_metrics.time_frame = 'all'
                    THEN flattened_metrics.metrics_value
            END
        ),
        0
    ) AS stage_foundations_alltime_feature_sum,
    

    /* If want to calculate 28 day metrics, could use the lag function. Or
       compute by nesting this SELECT statement in a WITH and computing after
       the fact, STAGE_PLAN_ALLTIME_FEATURE_SUM -
       COALESCE(LAG(STAGE_PLAN_ALLTIME_FEATURE_SUM)
       OVER (PARTITION BY flattened_metrics.DIM_CRM_ACCOUNT_ID ORDER BY
       flattened_metrics.SNAPSHOT_MONTH), 0) as STAGE_PLAN_28DAYS_FEATURE_SUM
    */

    -- FEATURE USE SHARE BY STAGE
    SUM(
        CASE
            WHEN
                usage_ping_metrics.time_frame = 'all'
                THEN flattened_metrics.metrics_value
        END
    ) AS all_stages_alltime_feature_sum,
    ROUND(
        DIV0(stage_plan_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_plan_alltime_share_pct,
    ROUND(
        DIV0(stage_create_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_create_alltime_share_pct,
    ROUND(
        DIV0(stage_verify_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_verify_alltime_share_pct,
    ROUND(
        DIV0(stage_package_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_package_alltime_share_pct,
    ROUND(
        DIV0(stage_monitor_alltime_features_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_monitor_alltime_share_pct,
    ROUND(
        DIV0(stage_secure_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_secure_alltime_share_pct,
    ROUND(
        DIV0(stage_govern_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_govern_alltime_share_pct,
    ROUND(
        DIV0(stage_deploy_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_deploy_alltime_share_pct,
    ROUND(
        DIV0(stage_foundations_alltime_feature_sum,
             all_stages_alltime_feature_sum), 4
    ) AS stage_foundations_alltime_share_pct,


    -- MOST USED STAGE ALL TIME
    CASE GREATEST(
        stage_plan_alltime_share_pct,
        stage_create_alltime_share_pct,
        stage_verify_alltime_share_pct,
        stage_package_alltime_share_pct,
        stage_monitor_alltime_share_pct,
        stage_secure_alltime_share_pct,
        stage_govern_alltime_share_pct,
        stage_deploy_alltime_share_pct,
        stage_foundations_alltime_share_pct

    )
        WHEN stage_plan_alltime_share_pct THEN 'plan'
        WHEN stage_create_alltime_share_pct THEN 'create'
        WHEN stage_verify_alltime_share_pct THEN 'verify'
        WHEN stage_package_alltime_share_pct THEN 'package'
        WHEN stage_monitor_alltime_share_pct THEN 'monitor'
        WHEN stage_secure_alltime_share_pct THEN 'secure'
        WHEN stage_govern_alltime_share_pct THEN 'govern'
        WHEN stage_deploy_alltime_share_pct THEN 'deploy'
        WHEN stage_foundations_alltime_share_pct THEN 'foundations'
        ELSE 'none'
    END AS stage_most_used_alltime,


    -- NUMBER OF SEAT LICENSES USING EACH STAGE
    -- Cannot get at because of the level of granuality of the usage
    -- datflattened_metrics.

    -- TOTAL MONTHS USED BY STAGES
    CASE WHEN stage_plan_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_plan_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_plan_months_used,
    CASE WHEN stage_create_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_create_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_create_months_used,
    CASE WHEN stage_verify_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_verify_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_verify_months_used,
    CASE WHEN stage_package_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_package_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_package_months_used,
    CASE WHEN stage_monitor_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_monitor_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_monitor_months_used,
    CASE WHEN stage_secure_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_secure_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_secure_months_used,
 
    CASE WHEN stage_govern_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_govern_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_govern_months_used,
    CASE WHEN stage_deploy_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_deploy_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_deploy_months_used,
    CASE WHEN stage_foundations_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_crm_account_id,
                    CASE WHEN stage_foundations_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_foundations_months_used


FROM flattened_metrics
LEFT JOIN
    usage_ping_metrics ON
        flattened_metrics.metrics_path = usage_ping_metrics.metrics_path
WHERE usage_ping_metrics.metrics_status = 'active'
      AND flattened_metrics.dim_crm_account_id IS NOT NULL
GROUP BY
    flattened_metrics.dim_crm_account_id,
    flattened_metrics.snapshot_month

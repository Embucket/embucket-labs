{{
    config(
        materialized='incremental',
        unique_key='primary_key'
    )
}}

WITH flattened_metrics AS (

	SELECT *
	FROM {{ ref('prep_saas_flattened_metrics') }}
	{% if is_incremental() %}
	WHERE snapshot_month >= (SELECT MAX(snapshot_month) FROM {{ this }})
	{% endif %}

), usage_ping_metrics AS (
  
		SELECT *
		FROM {{ ref('dim_ping_metric') }}

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['flattened_metrics.snapshot_month', 'flattened_metrics.dim_namespace_id']) }} AS primary_key,
	flattened_metrics.snapshot_month,
	flattened_metrics.dim_namespace_id,

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
       OVER (PARTITION BY flattened_metrics.dim_namespace_id ORDER BY
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
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_plan_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_plan_months_used,
    CASE WHEN stage_create_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_create_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_create_months_used,
    CASE WHEN stage_verify_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_verify_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_verify_months_used,
    CASE WHEN stage_package_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_package_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_package_months_used,
    CASE WHEN stage_monitor_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_monitor_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_monitor_months_used,
    CASE WHEN stage_secure_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_secure_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_secure_months_used,
 
    CASE WHEN stage_govern_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_govern_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_govern_months_used,
    CASE WHEN stage_deploy_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_deploy_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_deploy_months_used,
    CASE WHEN stage_foundations_28days_features = 0 THEN 0
        ELSE
            ROW_NUMBER() OVER (
                PARTITION BY
                    flattened_metrics.dim_namespace_id,
                    CASE WHEN stage_foundations_28days_features > 0 THEN 1 END
                ORDER BY flattened_metrics.snapshot_month
            )
    END AS stage_foundations_months_used


FROM flattened_metrics
LEFT JOIN usage_ping_metrics
  ON flattened_metrics.metrics_path = usage_ping_metrics.metrics_path
WHERE usage_ping_metrics.metrics_status = 'active'

{{ dbt_utils.group_by(n=3) }}

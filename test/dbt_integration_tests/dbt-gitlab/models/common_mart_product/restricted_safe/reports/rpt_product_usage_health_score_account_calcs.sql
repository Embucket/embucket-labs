{{ config(
  materialized='table',
  snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{ simple_cte([
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges'),
    ('dim_crm_account','dim_crm_account')
]) }},

fy25_account_ranking AS (

  SELECT
    dim_crm_account_id,
    SUM(arr)                                      AS total_account_arr,
    RANK() OVER (ORDER BY total_account_arr DESC) AS fy25_account_rank,
    IFF(fy25_account_rank <= 100, TRUE, FALSE)    AS is_fy25_top_100_account
  FROM mart_arr_all
  WHERE arr_month = '2024-01-01'
    AND dim_crm_account_id != '0016100001TBkZNAA1' --exclude partner accounts
  GROUP BY 1

),

monthly_account_arr AS (

  SELECT
    mart_arr_all.arr_month,
    mart_arr_all.dim_crm_account_id,
    mart_arr_all.crm_account_name,
    fy25_account_ranking.fy25_account_rank,
    fy25_account_ranking.is_fy25_top_100_account,
    dim_crm_account.customer_since_date,
    DATEDIFF(MONTH, dim_crm_account.customer_since_date, mart_arr_all.arr_month)                         AS account_age_months,
    SUM(mart_arr_all.arr)                                                                                AS total_account_subscription_arr,
    SUM(IFF(mart_arr_all.product_category = 'Base Products',arr,0))                                       AS total_account_base_products_arr,
    SUM(IFF(mart_arr_all.product_tier_name ILIKE '%Ultimate%', arr,0))                                     AS total_account_ultimate_arr,
    SUM(IFF(mart_arr_all.product_category != 'Base Products',arr,0))                                      AS total_account_add_on_arr,
    COUNT(DISTINCT mart_arr_all.dim_subscription_id_original)                                                         AS number_of_subscriptions,
    COUNT(DISTINCT IFF(mart_arr_all.product_category = 'Base Products', dim_subscription_id_original, NULL))   AS number_of_base_products_subscriptions,
    COUNT(DISTINCT IFF(mart_arr_all.product_tier_name ILIKE '%Ultimate%', dim_subscription_id_original, NULL)) AS number_of_ultimate_subscriptions,
    COUNT(DISTINCT IFF(mart_arr_all.product_category != 'Base Products', dim_subscription_id_original, NULL))  AS number_of_add_on_subscriptions
  FROM mart_arr_all
  LEFT JOIN dim_crm_account
    ON mart_arr_all.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN fy25_account_ranking
    ON mart_arr_all.dim_crm_account_id = fy25_account_ranking.dim_crm_account_id
  WHERE arr_month < CURRENT_DATE
  GROUP BY 1, 2, 3, 4, 5, 6, 7

),

monthly_subscription_base_products_arr AS (

  SELECT
    mart_arr_all.arr_month,
    mart_arr_all.dim_crm_account_id,
    mart_arr_all.dim_subscription_id_original,
    mart_arr_all.subscription_name,
    mart_arr_all.product_delivery_type,
    mart_arr_all.product_deployment_type,
    LISTAGG(DISTINCT mart_arr_all.product_tier_name, ', ') AS product_tier_name_string_agg,
    SUM(mart_arr_all.arr)                     AS total_subscription_base_products_arr
  FROM mart_arr_all
  WHERE product_category = 'Base Products'
    AND arr_month < CURRENT_DATE
  GROUP BY 1, 2, 3, 4, 5, 6

),

production_usage_data AS (

  SELECT *
  FROM rpt_product_usage_health_score
  WHERE instance_type = 'Production'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type ORDER BY billable_user_count DESC NULLS LAST, ping_created_at DESC NULLS LAST) = 1

),

subscription_level_calculations AS (

  SELECT DISTINCT
    monthly_account_arr.arr_month,
    monthly_account_arr.dim_crm_account_id,
    monthly_account_arr.crm_account_name,
    monthly_account_arr.fy25_account_rank,
    monthly_account_arr.is_fy25_top_100_account,
    monthly_account_arr.customer_since_date,
    monthly_account_arr.account_age_months,
    monthly_account_arr.total_account_subscription_arr,
    monthly_account_arr.total_account_base_products_arr,
    monthly_account_arr.total_account_ultimate_arr,
    monthly_account_arr.total_account_add_on_arr,
    monthly_account_arr.number_of_subscriptions,
    monthly_account_arr.number_of_base_products_subscriptions,
    monthly_account_arr.number_of_ultimate_subscriptions,
    monthly_account_arr.number_of_add_on_subscriptions,
    monthly_subscription_base_products_arr.dim_subscription_id_original,
    monthly_subscription_base_products_arr.subscription_name,
    monthly_subscription_base_products_arr.product_delivery_type,
    monthly_subscription_base_products_arr.product_deployment_type,
    monthly_subscription_base_products_arr.product_tier_name_string_agg,
    monthly_subscription_base_products_arr.total_subscription_base_products_arr,
    production_usage_data.ping_created_at,
    IFF(production_usage_data.ping_created_at IS NULL, FALSE, TRUE)                                                                                                                                                  AS is_reporting_usage_data,
    SPLIT_PART(production_usage_data.cleaned_version, '.', 1)                                                                                                                                                        AS major_version,
    SPLIT_PART(production_usage_data.cleaned_version, '.', 2)                                                                                                                                                        AS minor_version,
    SPLIT_PART(production_usage_data.cleaned_version, '.', 3)                                                                                                                                                        AS double_minor_version,
    (production_usage_data.license_utilization) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)                                                                                      AS subscription_license_utilization_dollars,
    (production_usage_data.user_engagement) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)                                                                                          AS subscription_user_engagement_dollars,
    (production_usage_data.git_operation_utilization) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)                                                                                AS subscription_scm_utilization_dollars,
    (production_usage_data.ci_pipeline_utilization) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)                                                                                  AS subscription_ci_utilization_dollars,
	(production_usage_data.ci_builds_per_billable_user_score) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)																		 AS subscription_ci_builds_per_billable_user_dollars,
    (production_usage_data.cd_score) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)                                                                                                 AS subscription_cd_utilization_dollars,
    IFF(monthly_subscription_base_products_arr.product_tier_name_string_agg ILIKE '%Ultimate%', ((production_usage_data.security_score) * (monthly_subscription_base_products_arr.total_subscription_base_products_arr)), NULL) AS subscription_security_utilization_dollars,
    production_usage_data.duo_pro_license_user_count,
    production_usage_data.duo_pro_billable_user_count,
    production_usage_data.duo_pro_license_utilization,
    production_usage_data.duo_enterprise_license_user_count,
    production_usage_data.duo_enterprise_billable_user_count,
    production_usage_data.duo_enterprise_license_utilization,
    production_usage_data.duo_total_license_user_count,
    production_usage_data.duo_total_billable_user_count,
    production_usage_data.duo_total_license_utilization
  FROM monthly_account_arr
  LEFT JOIN monthly_subscription_base_products_arr
    ON monthly_account_arr.dim_crm_account_id = monthly_subscription_base_products_arr.dim_crm_account_id
      AND monthly_account_arr.arr_month = monthly_subscription_base_products_arr.arr_month
  LEFT JOIN production_usage_data
    ON monthly_subscription_base_products_arr.dim_subscription_id_original = production_usage_data.dim_subscription_id_original
      AND monthly_subscription_base_products_arr.arr_month = production_usage_data.snapshot_month
      AND monthly_subscription_base_products_arr.product_delivery_type = production_usage_data.delivery_type

),

account_rollup_calculations AS (

  SELECT
    arr_month,
    dim_crm_account_id,
    crm_account_name,
    fy25_account_rank,
    is_fy25_top_100_account,
    account_age_months,
    total_account_subscription_arr,
    total_account_base_products_arr,
    total_account_ultimate_arr,
    total_account_add_on_arr,
    number_of_subscriptions,
    number_of_base_products_subscriptions,
    number_of_ultimate_subscriptions,
    number_of_add_on_subscriptions,
    SUM(CASE WHEN is_reporting_usage_data = TRUE THEN total_subscription_base_products_arr END)                                                                      AS account_arr_reporting_usage_data,
    SUM(CASE WHEN is_reporting_usage_data = TRUE AND product_tier_name_string_agg ILIKE '%Ultimate%' THEN total_subscription_base_products_arr END)                             AS account_ultimate_arr_reporting_usage_data,
    ZEROIFNULL(DIV0(account_arr_reporting_usage_data, total_account_base_products_arr))                                                                              AS pct_of_account_arr_reporting_usage_data,
    DIV0(account_ultimate_arr_reporting_usage_data, total_account_ultimate_arr)                                                                                      AS pct_of_account_ultimate_arr_reporting_usage_data,
    IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_license_utilization_dollars), account_arr_reporting_usage_data))                    AS account_weighted_license_utilization,
    IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_user_engagement_dollars), account_arr_reporting_usage_data))                        AS account_weighted_user_engagement,
    IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_scm_utilization_dollars), account_arr_reporting_usage_data))                        AS account_weighted_scm_utilization,
    IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_ci_utilization_dollars), account_arr_reporting_usage_data))                         AS account_weighted_ci_utilization,
	IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_ci_builds_per_billable_user_dollars), account_arr_reporting_usage_data))			 AS account_weighted_ci_builds_per_billable_user,
    IFF(pct_of_account_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_cd_utilization_dollars), account_arr_reporting_usage_data))                         AS account_weighted_cd_utilization,
    IFF(pct_of_account_ultimate_arr_reporting_usage_data = 0, NULL, DIV0(SUM(subscription_security_utilization_dollars), account_ultimate_arr_reporting_usage_data)) AS account_weighted_security_utilization,
    (CASE WHEN account_age_months < 3 THEN NULL
      WHEN account_age_months >= 3 AND account_age_months < 7
        THEN (CASE WHEN account_weighted_license_utilization <= 0.10 THEN 25
          WHEN account_weighted_license_utilization > 0.10 AND account_weighted_license_utilization <= 0.50 THEN 63
          WHEN account_weighted_license_utilization > 0.50 THEN 88
        END)
      WHEN account_age_months >= 7 AND account_age_months < 10
        THEN (CASE WHEN account_weighted_license_utilization <= 0.50 THEN 25
          WHEN account_weighted_license_utilization > 0.50 AND account_weighted_license_utilization <= 0.75 THEN 63
          WHEN account_weighted_license_utilization > 0.75 THEN 88
        END)
      WHEN account_age_months >= 10 THEN (CASE WHEN account_weighted_license_utilization <= 0.75 THEN 25
        WHEN account_weighted_license_utilization > 0.75 THEN 88
      END)
    END)                                                                                                                                                             AS account_level_license_utilization_score,
    CASE WHEN account_level_license_utilization_score IS NULL THEN NULL
      WHEN account_level_license_utilization_score = 25 THEN 'Red'
      WHEN account_level_license_utilization_score = 63 THEN 'Yellow'
      WHEN account_level_license_utilization_score = 88 THEN 'Green'
    END                                                                                                                                                              AS account_level_license_utilization_color,
    CASE WHEN account_weighted_user_engagement IS NULL THEN NULL
      WHEN account_weighted_user_engagement >= 0.80 THEN 'Green'
      WHEN account_weighted_user_engagement >= 0.50 AND account_weighted_user_engagement < 0.80 THEN 'Yellow'
      WHEN account_weighted_user_engagement < 0.50 THEN 'Red'
    END                                                                                                                                                              AS account_level_user_engagement_color,
    CASE WHEN account_weighted_scm_utilization >= 0.50 THEN 'Green'
      WHEN account_weighted_scm_utilization >= 0.25 AND account_weighted_scm_utilization < 0.50 THEN 'Yellow'
      WHEN account_weighted_scm_utilization < 0.25 THEN 'Red'
      ELSE 'No Data'
    END                                                                                                                                                              AS account_level_scm_color,
    CASE WHEN account_weighted_ci_utilization > 0.333 THEN 'Green'
      WHEN account_weighted_ci_utilization > 0.10 AND account_weighted_ci_utilization <= 0.333 THEN 'Yellow'
      WHEN account_weighted_ci_utilization <= 0.10 THEN 'Red'
      ELSE 'No Data'
    END                                                                                                                                                              AS account_level_ci_color,
    CASE WHEN account_weighted_ci_builds_per_billable_user > 75 THEN 'Green'
        WHEN account_weighted_ci_builds_per_billable_user > 50 AND account_weighted_ci_builds_per_billable_user <=75 THEN 'Yellow'
        WHEN account_weighted_ci_builds_per_billable_user <= 50 THEN 'Red'
        ELSE 'No Data'
    END																																								 AS account_level_ci_lighthouse_color,
    CASE WHEN account_weighted_cd_utilization > 75 THEN 'Green'
      WHEN account_weighted_cd_utilization > 50 AND account_weighted_cd_utilization <= 75 THEN 'Yellow'
      WHEN account_weighted_cd_utilization <= 50 THEN 'Red'
      ELSE 'No Data'
    END                                                                                                                                                              AS account_level_cd_color,
    CASE WHEN total_account_ultimate_arr IS NULL THEN 'N/A'
      WHEN account_weighted_security_utilization > 75 THEN 'Green'
      WHEN account_weighted_security_utilization > 50 AND account_weighted_security_utilization <= 75 THEN 'Yellow'
      WHEN account_weighted_security_utilization <= 50 THEN 'Red'
      ELSE 'No Data'
    END                                                                                                                                                              AS account_level_security_color,
    MAX(ping_created_at)                                                                                                                                             AS max_ping_created_at
  FROM subscription_level_calculations
  GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14

),

final AS (

  SELECT
    rpt_product_usage_health_score.* EXCLUDE (
        created_by,
        updated_by,
        model_created_date,
        model_updated_date,
        dbt_updated_at,
        dbt_created_at,
        primary_key
        ),
    COALESCE(account_rollup_calculations.arr_month,rpt_product_usage_health_score.snapshot_month) AS reporting_month,
    account_rollup_calculations.arr_month,
    account_rollup_calculations.dim_crm_account_id AS dim_crm_account_id_mart_arr_all,
    account_rollup_calculations.crm_account_name AS mart_arr_all_account_name,
    account_rollup_calculations.fy25_account_rank,
    account_rollup_calculations.is_fy25_top_100_account,
    account_rollup_calculations.total_account_subscription_arr,
    account_rollup_calculations.total_account_base_products_arr,
    account_rollup_calculations.total_account_ultimate_arr,
    account_rollup_calculations.total_account_add_on_arr,
    account_rollup_calculations.number_of_subscriptions,
    account_rollup_calculations.number_of_base_products_subscriptions,
    account_rollup_calculations.number_of_ultimate_subscriptions,
    account_rollup_calculations.number_of_add_on_subscriptions,
    account_rollup_calculations.account_arr_reporting_usage_data,
    account_rollup_calculations.account_ultimate_arr_reporting_usage_data,
    account_rollup_calculations.pct_of_account_arr_reporting_usage_data,
    account_rollup_calculations.pct_of_account_ultimate_arr_reporting_usage_data,
    account_rollup_calculations.account_weighted_license_utilization,
    account_rollup_calculations.account_weighted_user_engagement,
    account_rollup_calculations.account_weighted_scm_utilization,
    account_rollup_calculations.account_weighted_ci_utilization,
	account_rollup_calculations.account_weighted_ci_builds_per_billable_user,
    account_rollup_calculations.account_weighted_cd_utilization,
    account_rollup_calculations.account_weighted_security_utilization,
    account_rollup_calculations.account_level_license_utilization_color,
    account_rollup_calculations.account_level_user_engagement_color,
    account_rollup_calculations.account_level_scm_color,
    account_rollup_calculations.account_level_ci_color,
	account_rollup_calculations.account_level_ci_lighthouse_color,
    account_rollup_calculations.account_level_cd_color,
    account_rollup_calculations.account_level_security_color,
    account_rollup_calculations.max_ping_created_at,
    {{ dbt_utils.generate_surrogate_key(
        [
          'reporting_month',
          'dim_subscription_id',
          'deployment_type',
          'dim_installation_id',
          'dim_namespace_id',
          'account_rollup_calculations.dim_crm_account_id'
        ]
      ) }} AS primary_key
  FROM rpt_product_usage_health_score
  FULL OUTER JOIN account_rollup_calculations
    ON rpt_product_usage_health_score.dim_crm_account_id = account_rollup_calculations.dim_crm_account_id
      AND rpt_product_usage_health_score.snapshot_month = account_rollup_calculations.arr_month
)

SELECT *,
   LAG(account_level_ci_color) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_ci_color_previous_month,
   LAG(account_level_ci_color, 3) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_ci_color_previous_3_month,
   LAG(account_level_ci_lighthouse_color) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_ci_lighthouse_color_previous_month,
   LAG(account_level_ci_lighthouse_color, 3) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_ci_lighthouse_color_previous_3_month,
   LAG(account_level_scm_color) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_scm_color_previous_month,
   LAG(account_level_scm_color, 3) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_scm_color_previous_3_month,
   LAG(account_level_cd_color) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_cd_color_previous_month,
   LAG(account_level_cd_color, 3) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_cd_color_previous_3_month,
   LAG(account_level_security_color) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_security_color_previous_month,
   LAG(account_level_security_color, 3) OVER (PARTITION BY hostname_or_namespace_id ORDER BY snapshot_month) AS account_level_security_color_previous_3_month
FROM final

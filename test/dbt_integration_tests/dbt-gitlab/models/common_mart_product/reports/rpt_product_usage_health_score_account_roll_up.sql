{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('rpt_product_usage_health_score_account_calcs','rpt_product_usage_health_score_account_calcs')
]) }}

, duo_aggregates AS (

  SELECT
    reporting_month,
    dim_crm_account_id_mart_arr_all                                                 AS dim_crm_account_id,
    SUM(duo_pro_license_user_count)                                                 AS account_level_duo_pro_license_user_count,
    SUM(duo_pro_billable_user_count)                                                AS account_level_duo_pro_billable_user_count,
    SUM(duo_enterprise_license_user_count)                                          AS account_level_duo_enterprise_license_user_count,
    SUM(duo_enterprise_billable_user_count)                                         AS account_level_duo_enterprise_billable_user_count,
    SUM(duo_pro_billable_user_count)/SUM(duo_pro_license_user_count)                AS account_level_duo_pro_license_utilization,
    SUM(duo_enterprise_billable_user_count)/SUM(duo_enterprise_license_user_count)  AS account_level_duo_enterprise_license_utilization,
    SUM(duo_total_billable_user_count)                                              AS account_level_duo_total_billable_user_count,
    SUM(duo_total_license_user_count)                                               AS account_level_duo_total_license_user_count,
    SUM(duo_total_billable_user_count)/SUM(duo_total_license_user_count)            AS account_level_duo_total_license_utilization
  FROM rpt_product_usage_health_score_account_calcs
  WHERE is_primary_instance_subscription = TRUE
  {{ dbt_utils.group_by(n=2) }}

), final AS (

  SELECT DISTINCT
    rpt_product_usage_health_score_account_calcs.reporting_month,
    rpt_product_usage_health_score_account_calcs.dim_crm_account_id_mart_arr_all                            AS dim_crm_account_id,
    rpt_product_usage_health_score_account_calcs.max_ping_created_at,
    rpt_product_usage_health_score_account_calcs.account_arr_reporting_usage_data,
    rpt_product_usage_health_score_account_calcs.account_ultimate_arr_reporting_usage_data,
    rpt_product_usage_health_score_account_calcs.pct_of_account_arr_reporting_usage_data,
    rpt_product_usage_health_score_account_calcs.pct_of_account_ultimate_arr_reporting_usage_data,
    rpt_product_usage_health_score_account_calcs.account_weighted_license_utilization,
    rpt_product_usage_health_score_account_calcs.account_weighted_user_engagement,
    rpt_product_usage_health_score_account_calcs.account_weighted_scm_utilization,
    rpt_product_usage_health_score_account_calcs.account_weighted_ci_utilization,
	rpt_product_usage_health_score_account_calcs.account_weighted_ci_builds_per_billable_user,
    rpt_product_usage_health_score_account_calcs.account_weighted_cd_utilization,
    rpt_product_usage_health_score_account_calcs.account_weighted_security_utilization,
    rpt_product_usage_health_score_account_calcs.account_level_license_utilization_color,
    rpt_product_usage_health_score_account_calcs.account_level_user_engagement_color,
    rpt_product_usage_health_score_account_calcs.account_level_scm_color,
    rpt_product_usage_health_score_account_calcs.account_level_ci_color,
	rpt_product_usage_health_score_account_calcs.account_level_ci_lighthouse_color,
    rpt_product_usage_health_score_account_calcs.account_level_cd_color,
    rpt_product_usage_health_score_account_calcs.account_level_security_color,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_license_utilization_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_license_utilization_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_license_utilization_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_license_utilization_color_value,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_user_engagement_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_user_engagement_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_user_engagement_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_user_engagement_color_value,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_scm_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_scm_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_scm_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_scm_color_value,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_ci_color_value,
	CASE
	  WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_lighthouse_color = 'Green'
		THEN 88
	  WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_lighthouse_color = 'Yellow'
		THEN 63
	  WHEN rpt_product_usage_health_score_account_calcs.account_level_ci_lighthouse_color = 'Red'
	 	THEN 25
	END																										AS gs_ci_lighthouse_color_value,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_cd_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_cd_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_cd_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_cd_color_value,
    CASE
      WHEN rpt_product_usage_health_score_account_calcs.account_level_security_color = 'Green'
        THEN 88
      WHEN rpt_product_usage_health_score_account_calcs.account_level_security_color = 'Yellow'
        THEN 63
      WHEN rpt_product_usage_health_score_account_calcs.account_level_security_color = 'Red'
        THEN 25
    END                                                                                                     AS gs_security_color_value,
    duo_aggregates.account_level_duo_pro_license_user_count,
    duo_aggregates.account_level_duo_pro_billable_user_count,
    duo_aggregates.account_level_duo_enterprise_license_user_count,
    duo_aggregates.account_level_duo_enterprise_billable_user_count,
    duo_aggregates.account_level_duo_pro_license_utilization,
    duo_aggregates.account_level_duo_enterprise_license_utilization
  FROM rpt_product_usage_health_score_account_calcs
  LEFT JOIN duo_aggregates
    ON rpt_product_usage_health_score_account_calcs.reporting_month = duo_aggregates.reporting_month
      AND rpt_product_usage_health_score_account_calcs.dim_crm_account_id_mart_arr_all = duo_aggregates.dim_crm_account_id

)

SELECT *
FROM final

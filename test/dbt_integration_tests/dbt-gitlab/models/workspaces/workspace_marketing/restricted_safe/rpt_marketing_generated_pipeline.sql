{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('mart_crm_opportunity_daily_snapshot','mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_date','dim_date')
]) }}

,  snapshot_dates AS (
--Snapshot on the 4th day of the current quarter for final previous quarter's numbers.
  SELECT
    date_day,
    LAG(fiscal_year, 1) OVER (ORDER BY date_day) AS fiscal_year,
    LAG(fiscal_quarter, 1) OVER (ORDER BY date_day) AS fiscal_quarter,
    LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) AS fiscal_quarter_name_fy
  FROM dim_date 
  WHERE (day_of_fiscal_quarter = 4 OR (current_day_of_fiscal_quarter-1 < 4 AND fiscal_quarters_ago = 0))
    AND date_day BETWEEN '2023-01-31' AND current_date-1
  QUALIFY LAG(fiscal_quarter_name_fy, 1) OVER (ORDER BY date_day) IS NOT NULL
  
  UNION 
--Latest snapshot for current quarter
  SELECT DISTINCT
  date_day,
  fiscal_year,
  fiscal_quarter,
  fiscal_quarter_name_fy
  FROM dim_date 
  WHERE day_of_fiscal_quarter > 4 
    AND date_day = CURRENT_DATE-1
  ORDER BY 1 DESC
  ),

opportunity_snapshot_base AS (
  SELECT
    snapshot.*,
    dim_date.day_of_fiscal_quarter_normalised    AS pipeline_created_day_of_fiscal_quarter_normalised,
    dim_date.day_of_fiscal_year_normalised       AS pipeline_created_day_of_fiscal_year_normalised
  FROM mart_crm_opportunity_daily_snapshot        AS snapshot
  INNER JOIN snapshot_dates
    ON snapshot.snapshot_date = snapshot_dates.date_day
  LEFT JOIN dim_date 
    ON snapshot.pipeline_created_date = dim_date.date_day
  WHERE snapshot_dates.fiscal_quarter_name_fy = snapshot.pipeline_created_fiscal_quarter_name 
    AND snapshot.dim_crm_account_id != '0014M00001kGcORQA0'  -- test account
),

final AS (
  SELECT 
    -- IDs
    opportunity.dim_crm_opportunity_id,
    opportunity.dim_crm_account_id,
    opportunity.dim_parent_crm_account_id,
    touchpoint.dim_crm_touchpoint_id,

    -- Dates
    opportunity.created_date,
    opportunity.sales_accepted_date,
    opportunity.pipeline_created_date,
    opportunity.pipeline_created_fiscal_quarter_name,
    opportunity.pipeline_created_fiscal_year,
    opportunity.pipeline_created_day_of_fiscal_quarter_normalised,
    opportunity.pipeline_created_day_of_fiscal_year_normalised,
    opportunity.close_date,
    opportunity.close_fiscal_quarter_name,
    touchpoint.bizible_touchpoint_date,
    opportunity.snapshot_date                     AS opportunity_snapshot_date,

    -- Account Info
    opportunity.parent_crm_account_sales_segment,
    opportunity.parent_crm_account_geo,
    opportunity.parent_crm_account_region,
    opportunity.parent_crm_account_area,
    opportunity.crm_account_name                  AS account_name,
    opportunity.parent_crm_account_name,

    -- Opportunity Dimensions
    opportunity.opportunity_category,
    opportunity.subscription_type,
    opportunity.order_type,
    opportunity.sales_qualified_source_name,
    opportunity.stage_name,
    opportunity.report_segment,
    opportunity.report_geo,
    opportunity.report_area,
    opportunity.report_region,
    opportunity.parent_crm_account_geo_pubsec_segment,
    opportunity.report_role_level_1,
    opportunity.report_role_level_2,
    SPLIT_PART(opportunity.report_role_level_2, '_', 2)      AS report_role_level_2_clean,
    opportunity.report_role_level_3,
    COALESCE(
      SPLIT_PART(opportunity.report_role_level_3, '_', 3),
      SPLIT_PART(opportunity.report_role_level_2, '_', 2)
    )                                                        AS report_role_level_3_clean,
    opportunity.report_role_level_4,
    opportunity.report_role_level_5,
    opportunity.pipe_council_grouping,

    -- Touchpoint Dimensions
    CASE 
      WHEN opportunity.sales_qualified_source_name = 'SDR Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'SDR Generated'
      WHEN opportunity.sales_qualified_source_name = 'Web Direct Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'Web Direct'
      ELSE touchpoint.bizible_touchpoint_type 
    END                                                     AS bizible_touchpoint_type,
    touchpoint.bizible_integrated_campaign_grouping,
    touchpoint.touchpoint_sales_stage                       AS opp_touchpoint_sales_stage,
    CASE 
      WHEN opportunity.sales_qualified_source_name = 'SDR Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'SDR Generated'
      WHEN opportunity.sales_qualified_source_name = 'Web Direct Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'Web Direct'
      ELSE touchpoint.bizible_marketing_channel
    END                                                     AS bizible_marketing_channel,
    CASE 
      WHEN opportunity.sales_qualified_source_name = 'SDR Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'SDR Generated.No Touchpoint'
      WHEN opportunity.sales_qualified_source_name = 'Web Direct Generated' 
        AND touchpoint.dim_crm_touchpoint_id IS NULL
        THEN 'Web Direct.No Touchpoint'
      ELSE touchpoint.bizible_marketing_channel_path
    END                                                     AS bizible_marketing_channel_path,
    touchpoint.marketing_review_channel_grouping,
    touchpoint.bizible_ad_campaign_name,
    touchpoint.bizible_form_url,
    touchpoint.budget_holder,
    touchpoint.campaign_rep_role_name,
    touchpoint.campaign_region,
    touchpoint.campaign_sub_region,
    touchpoint.budgeted_cost,
    touchpoint.actual_cost,
    touchpoint.utm_campaign,
    touchpoint.utm_source,
    touchpoint.utm_medium,
    touchpoint.utm_content,
    touchpoint.utm_budget,
    touchpoint.utm_allptnr,
    touchpoint.utm_partnerid,
    touchpoint.devrel_campaign_type,
    touchpoint.devrel_campaign_description,
    touchpoint.devrel_campaign_influence_type,
    touchpoint.integrated_budget_holder,
    touchpoint.type                                         AS sfdc_campaign_type,
    touchpoint.gtm_motion,
    touchpoint.account_demographics_sales_segment           AS person_sales_segment,
    touchpoint.touchpoint_offer_type,
    touchpoint.touchpoint_offer_type_grouped,
    touchpoint.is_mgp_opportunity,
    touchpoint.is_mgp_channel_based,
    -- Model Weights
    touchpoint.bizible_count_custom_model                   AS custom_model_weight,
    touchpoint.gitlab_model_weight,
    touchpoint.time_decay_model_weight,
    touchpoint.data_driven_model_weight,
    -- Metrics
    opportunity.net_arr                                     AS opp_net_arr,
    COALESCE(
      touchpoint.bizible_count_custom_model, 
      CASE 
        WHEN opportunity.sales_qualified_source_name IN ('SDR Generated', 'Web Direct Generated')
          THEN 1
        ELSE 0
      END
    )                                                       AS bizible_count_custom_model,
    opportunity.net_arr * bizible_count_custom_model        AS custom_net_arr,
    opportunity.net_arr * COALESCE(touchpoint.gitlab_model_weight, 0) 
                                                           AS gitlab_model_net_arr,
    opportunity.net_arr * COALESCE(touchpoint.time_decay_model_weight, 0) 
                                                           AS time_decay_model_net_arr,
    opportunity.net_arr * COALESCE(touchpoint.data_driven_model_weight, 0) 
                                                           AS data_driven_model_net_arr,
    
    -- Flags
    opportunity.is_sao,
    opportunity.is_won,
    opportunity.is_web_portal_purchase,
    opportunity.fpa_master_bookings_flag,
    opportunity.is_edu_oss,
    opportunity.is_net_arr_pipeline_created                 AS is_eligible_created_pipeline_flag,
    opportunity.is_open,
    opportunity.is_lost,
    opportunity.is_closed,
    opportunity.is_renewal,
    opportunity.is_refund,
    opportunity.is_credit                                   AS is_credit_flag,
    opportunity.is_eligible_open_pipeline                   AS is_eligible_open_pipeline_flag,
    opportunity.is_net_arr_closed_deal                      AS is_booked_net_arr_flag,
    opportunity.is_eligible_age_analysis                    AS is_eligible_age_analysis_flag

  FROM opportunity_snapshot_base                            AS opportunity
  LEFT JOIN mart_crm_attribution_touchpoint                 AS touchpoint 
    ON opportunity.dim_crm_opportunity_id = touchpoint.dim_crm_opportunity_id
)

SELECT *
FROM final
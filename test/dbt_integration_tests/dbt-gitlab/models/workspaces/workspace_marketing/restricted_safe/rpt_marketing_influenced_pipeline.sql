{{ config(materialized='table') }}



{{ simple_cte([
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('mart_crm_opportunity_daily_snapshot','mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('mart_crm_account','mart_crm_account'),
    ('sfdc_bizible_attribution_touchpoint_snapshots_source', 'sfdc_bizible_attribution_touchpoint_snapshots_source'),
    ('dim_campaign','dim_campaign'),
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

),  attribution_touchpoint_snapshot_base AS (

    SELECT DISTINCT
    snapshot_dates.date_day AS touchpoint_snapshot_date,
    snapshot_dates.fiscal_quarter_name_fy AS snapshot_quarter_name,
    sfdc_bizible_attribution_touchpoint_snapshots_source.touchpoint_id AS dim_crm_touchpoint_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.opportunity_id AS dim_crm_opportunity_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_touchpoint_date,
    mart_crm_attribution_touchpoint.bizible_touchpoint_type,
    mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
    mart_crm_attribution_touchpoint.bizible_marketing_channel,
    mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_marketing_channel AS snapshot_marketing_channel,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_marketing_channel_path AS snapshot_marketing_channel_path,
    mart_crm_attribution_touchpoint.marketing_review_channel_grouping,
    mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
    mart_crm_attribution_touchpoint.bizible_form_url,
    mart_crm_attribution_touchpoint.budget_holder,
    mart_crm_attribution_touchpoint.campaign_rep_role_name,
    mart_crm_attribution_touchpoint.campaign_region,
    mart_crm_attribution_touchpoint.campaign_sub_region,
    mart_crm_attribution_touchpoint.budgeted_cost,
    mart_crm_attribution_touchpoint.actual_cost,
    mart_crm_attribution_touchpoint.utm_campaign,
    mart_crm_attribution_touchpoint.utm_medium,
    mart_crm_attribution_touchpoint.utm_source,
    mart_crm_attribution_touchpoint.utm_budget,
    mart_crm_attribution_touchpoint.utm_content,
    mart_crm_attribution_touchpoint.utm_allptnr,
    mart_crm_attribution_touchpoint.utm_partnerid,
    mart_crm_attribution_touchpoint.devrel_campaign_type,
    mart_crm_attribution_touchpoint.devrel_campaign_description,
    mart_crm_attribution_touchpoint.devrel_campaign_influence_type,
    mart_crm_attribution_touchpoint.integrated_budget_holder,
    mart_crm_attribution_touchpoint.type AS sfdc_campaign_type,
    mart_crm_attribution_touchpoint.gtm_motion,
    mart_crm_attribution_touchpoint.account_demographics_sales_segment AS person_sales_segment,
    mart_crm_attribution_touchpoint.touchpoint_offer_type,
    mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
    mart_crm_attribution_touchpoint.keystone_content_name,
    mart_crm_attribution_touchpoint.keystone_gitlab_epic,
    mart_crm_attribution_touchpoint.keystone_gtm,
    mart_crm_attribution_touchpoint.keystone_url_slug,
    mart_crm_attribution_touchpoint.keystone_type,
    dim_campaign.series_campaign_id,
    dim_campaign.series_campaign_name,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model/100 AS bizible_count_custom_model,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model,
    mart_crm_attribution_touchpoint.touchpoint_sales_stage AS opp_touchpoint_sales_stage
    FROM sfdc_bizible_attribution_touchpoint_snapshots_source
    INNER JOIN mart_crm_attribution_touchpoint 
      ON sfdc_bizible_attribution_touchpoint_snapshots_source.touchpoint_id = mart_crm_attribution_touchpoint.dim_crm_touchpoint_id
    INNER JOIN snapshot_dates 
      ON (dbt_valid_FROM <= date_day 
        AND dbt_valid_to > date_day) 
        OR (dbt_valid_from <= date_day 
          AND dbt_valid_to is null)
    LEFT JOIN mart_crm_opportunity 
      ON sfdc_bizible_attribution_touchpoint_snapshots_source.opportunity_id = mart_crm_opportunity.DIM_CRM_OPPORTUNITY_ID
    LEFT JOIN dim_campaign
      ON sfdc_bizible_attribution_touchpoint_snapshots_source.campaign_id= dim_campaign.dim_campaign_id
    WHERE snapshot_dates.fiscal_quarter_name_fy = mart_crm_opportunity.pipeline_created_fiscal_quarter_name 

)

, opportunity_snapshot_base AS (

SELECT
  snapshot.dim_crm_opportunity_id,
  snapshot.dim_crm_account_id,
  account.crm_account_name                  AS account_name,
  snapshot.dim_parent_crm_account_id,
  account.parent_crm_account_name,
  live.opportunity_category,
  live.subscription_type,
  live.order_type,
  live.sales_qualified_source_name,
  snapshot.stage_name,

  --Account Info
  account.owner_role                        AS account_owner_role,
  account.parent_crm_account_territory,
  live.parent_crm_account_sales_segment,
  live.parent_crm_account_geo,
  live.parent_crm_account_region,
  live.parent_crm_account_area,
  live.opportunity_owner_role,

  --Dates 
  snapshot.created_date,
  snapshot.sales_accepted_date,
  snapshot.pipeline_created_date,
  snapshot.pipeline_created_fiscal_quarter_name,
  snapshot.pipeline_created_fiscal_year,
  snapshot.net_arr_created_date,
  snapshot.close_date,
  snapshot.close_fiscal_quarter_name,
  snapshot.snapshot_date                    AS opportunity_snapshot_date,
  dim_date.day_of_fiscal_quarter_normalised AS pipeline_created_day_of_fiscal_quarter_normalised,
  dim_date.day_of_fiscal_year_normalised    AS pipeline_created_day_of_fiscal_year_normalised,

  --User Hierarchy
  snapshot.report_segment                   AS snapshot_report_segment,
  live.report_segment,
  snapshot.report_geo                       AS snapshot_report_geo,
  live.report_geo,
  snapshot.report_region                    AS snapshot_report_region,
  live.report_region,
  snapshot.report_area                      AS snapshot_report_area,
  live.report_area,
  snapshot.parent_crm_account_geo_pubsec_segment AS snapshot_parent_crm_account_geo_pubsec_segment,
  live.parent_crm_account_geo_pubsec_segment,
  --    report_opportunity_user_business_unit,
  --    report_opportunity_user_sub_business_unit,
  --    report_opportunity_user_division,
  --    report_opportunity_user_asm,
  live.report_role_level_1,
  live.report_role_level_2,
  SPLIT_PART(live.report_role_level_2, '_', 2)                         AS report_role_level_2_clean,
  live.report_role_level_3,
  COALESCE(SPLIT_PART(live.report_role_level_3, '_', 3),SPLIT_PART(live.report_role_level_2, '_', 2) )                         AS report_role_level_3_clean,
  live.report_role_level_4,
  live.report_role_level_5,
  live.pipe_council_grouping,

  --Flags
  live.is_sao,
  live.is_won,
  live.is_web_portal_purchase,
  live.is_edu_oss,
  live.is_net_arr_pipeline_created          AS is_eligible_created_pipeline_flag,
  live.is_open,
  live.is_lost,
  live.is_closed,
  live.is_renewal,
  live.is_refund,
  live.is_credit                            AS is_credit_flag,
  --    is_eligible_sao_flag,
  live.is_eligible_open_pipeline AS is_eligible_open_pipeline_flag,
  live.is_booked_net_arr AS is_booked_net_arr_flag,
  live.is_eligible_age_analysis AS is_eligible_age_analysis_flag,

  --Metrics
  snapshot.net_arr                          AS opp_net_arr

FROM mart_crm_opportunity_daily_snapshot AS snapshot
INNER JOIN snapshot_dates
  ON snapshot.snapshot_date = snapshot_dates.date_day
LEFT JOIN mart_crm_opportunity AS live
  ON snapshot.dim_crm_opportunity_id = live.dim_crm_opportunity_id
LEFT JOIN mart_crm_account AS account
  ON snapshot.dim_crm_account_id = account.dim_crm_account_id
LEFT JOIN dim_date 
  ON snapshot.pipeline_created_date = dim_date.date_day
WHERE snapshot_dates.fiscal_quarter_name_fy = snapshot.pipeline_created_fiscal_quarter_name 
  AND snapshot.dim_crm_account_id != '0014M00001kGcORQA0'  -- test account
), 


combined_models AS (

  SELECT
  --IDs
    opportunity_snapshot_base.dim_crm_opportunity_id,
    opportunity_snapshot_base.dim_crm_account_id,
    opportunity_snapshot_base.dim_parent_crm_account_id,
    attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id,

  --Dates
    opportunity_snapshot_base.created_date,
    opportunity_snapshot_base.sales_accepted_date,
    opportunity_snapshot_base.pipeline_created_date,
    opportunity_snapshot_base.pipeline_created_fiscal_quarter_name,
    opportunity_snapshot_base.pipeline_created_fiscal_year,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_quarter_normalised,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_year_normalised,
    opportunity_snapshot_base.net_arr_created_date,
    opportunity_snapshot_base.close_date,
    opportunity_snapshot_base.close_fiscal_quarter_name,
    attribution_touchpoint_snapshot_base.bizible_touchpoint_date,
    attribution_touchpoint_snapshot_base.touchpoint_snapshot_date,
    opportunity_snapshot_base.opportunity_snapshot_date,
  
  --Account Info
    opportunity_snapshot_base.account_owner_role,
    opportunity_snapshot_base.parent_crm_account_territory,
    opportunity_snapshot_base.parent_crm_account_sales_segment,
    opportunity_snapshot_base.parent_crm_account_geo,
    opportunity_snapshot_base.parent_crm_account_region,
    opportunity_snapshot_base.parent_crm_account_area,
    opportunity_snapshot_base.account_name,
    opportunity_snapshot_base.parent_crm_account_name,

--Opportunity Dimensions
    opportunity_snapshot_base.opportunity_category,
    opportunity_snapshot_base.subscription_type,
    opportunity_snapshot_base.order_type,
    opportunity_snapshot_base.sales_qualified_source_name,
    opportunity_snapshot_base.stage_name,
    opportunity_snapshot_base.report_segment,
    opportunity_snapshot_base.report_geo,
    opportunity_snapshot_base.report_area,
    opportunity_snapshot_base.parent_crm_account_geo_pubsec_segment,
    opportunity_snapshot_base.report_region,
    opportunity_snapshot_base.report_role_level_1,
    opportunity_snapshot_base.report_role_level_2,
    opportunity_snapshot_base.report_role_level_2_clean,
    opportunity_snapshot_base.report_role_level_3,
    opportunity_snapshot_base.report_role_level_3_clean,
    opportunity_snapshot_base.report_role_level_4,
    opportunity_snapshot_base.report_role_level_5,
    opportunity_snapshot_base.pipe_council_grouping,

--Touchpoint Dimensions
    attribution_touchpoint_snapshot_base.bizible_touchpoint_type,
    attribution_touchpoint_snapshot_base.bizible_integrated_campaign_grouping,
    attribution_touchpoint_snapshot_base.opp_touchpoint_sales_stage,
    CASE 
      WHEN opportunity_snapshot_base.sales_qualified_source_name = 'SDR Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'SDR Generated'
      WHEN opportunity_snapshot_base.sales_qualified_source_name = 'Web Direct Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'Web Direct'
      ELSE attribution_touchpoint_snapshot_base.bizible_marketing_channel 
    END AS bizible_marketing_channel,
    CASE 
      WHEN opportunity_snapshot_base.sales_qualified_source_name = 'SDR Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'SDR Generated.No Touchpoint'
      WHEN opportunity_snapshot_base.sales_qualified_source_name = 'Web Direct Generated' 
        AND attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id IS NULL
      THEN 'Web Direct.No Touchpoint'
      ELSE attribution_touchpoint_snapshot_base.bizible_marketing_channel_path 
    END AS bizible_marketing_channel_path,
    attribution_touchpoint_snapshot_base.snapshot_marketing_channel,
    attribution_touchpoint_snapshot_base.snapshot_marketing_channel_path,
    attribution_touchpoint_snapshot_base.marketing_review_channel_grouping,
    attribution_touchpoint_snapshot_base.bizible_ad_campaign_name,
    attribution_touchpoint_snapshot_base.bizible_form_url,
    attribution_touchpoint_snapshot_base.budget_holder,
    attribution_touchpoint_snapshot_base.campaign_rep_role_name,
    attribution_touchpoint_snapshot_base.campaign_region,
    attribution_touchpoint_snapshot_base.campaign_sub_region,
    attribution_touchpoint_snapshot_base.budgeted_cost,
    attribution_touchpoint_snapshot_base.actual_cost,
    attribution_touchpoint_snapshot_base.keystone_content_name,
    attribution_touchpoint_snapshot_base.keystone_gitlab_epic,
    attribution_touchpoint_snapshot_base.keystone_gtm,
    attribution_touchpoint_snapshot_base.keystone_url_slug,
    attribution_touchpoint_snapshot_base.keystone_type,
    attribution_touchpoint_snapshot_base.utm_campaign,
    attribution_touchpoint_snapshot_base.utm_source,
    attribution_touchpoint_snapshot_base.utm_medium,
    attribution_touchpoint_snapshot_base.utm_content,
    attribution_touchpoint_snapshot_base.utm_budget,
    attribution_touchpoint_snapshot_base.utm_allptnr,
    attribution_touchpoint_snapshot_base.utm_partnerid,
    attribution_touchpoint_snapshot_base.devrel_campaign_type,
    attribution_touchpoint_snapshot_base.devrel_campaign_description,
    attribution_touchpoint_snapshot_base.devrel_campaign_influence_type,
    attribution_touchpoint_snapshot_base.integrated_budget_holder,
    attribution_touchpoint_snapshot_base.sfdc_campaign_type,
    attribution_touchpoint_snapshot_base.gtm_motion,
    attribution_touchpoint_snapshot_base.person_sales_segment,
    attribution_touchpoint_snapshot_base.touchpoint_offer_type,
    attribution_touchpoint_snapshot_base.touchpoint_offer_type_grouped,
    attribution_touchpoint_snapshot_base.series_campaign_id,
    attribution_touchpoint_snapshot_base.series_campaign_name,

  --Metrics
    opportunity_snapshot_base.opp_net_arr,
    attribution_touchpoint_snapshot_base.bizible_count_custom_model,
    attribution_touchpoint_snapshot_base.bizible_weight_custom_model/100 * opportunity_snapshot_base.opp_net_arr AS custom_net_arr_base,
    CASE 
      WHEN opportunity_snapshot_base.sales_qualified_source_name IN ('SDR Generated','Web Direct Generated') AND dim_crm_touchpoint_id IS NULL 
      THEN opp_net_arr 
    ELSE custom_net_arr_base 
    END AS custom_net_arr,
    COALESCE(custom_net_arr,opp_net_arr) AS net_arr,
  --

  --Flags
    opportunity_snapshot_base.is_sao,
    opportunity_snapshot_base.is_won,
    opportunity_snapshot_base.is_web_portal_purchase,
    opportunity_snapshot_base.is_edu_oss,
    opportunity_snapshot_base.is_eligible_created_pipeline_flag,
    opportunity_snapshot_base.is_open,
    opportunity_snapshot_base.is_lost,
    opportunity_snapshot_base.is_closed,
    opportunity_snapshot_base.is_renewal,
    opportunity_snapshot_base.is_refund,
    opportunity_snapshot_base.is_credit_flag,
--    opportunity_snapshot_base.is_eligible_sao_flag, DM: removed during transition to mart_crm_opportunity_daily_snapshot
    opportunity_snapshot_base.is_eligible_open_pipeline_flag,
    opportunity_snapshot_base.is_booked_net_arr_flag,
    opportunity_snapshot_base.is_eligible_age_analysis_flag
    
  FROM opportunity_snapshot_base
  LEFT JOIN attribution_touchpoint_snapshot_base
    ON opportunity_snapshot_base.dim_crm_opportunity_id = attribution_touchpoint_snapshot_base.dim_crm_opportunity_id
    AND opportunity_snapshot_base.opportunity_snapshot_date = attribution_touchpoint_snapshot_base.touchpoint_snapshot_date

), missing_net_arr_difference AS (
    SELECT 
    dim_crm_opportunity_id, 
    pipeline_created_fiscal_quarter_name, 
    opp_net_arr, 
    sum(custom_net_arr) AS sum_custom_net_arr,
    opp_net_arr-sum_custom_net_arr AS net_arr_difference,
    DIV0(net_arr_difference,opp_net_arr) AS count_difference
    FROM 
    combined_models 
    WHERE 
    custom_net_arr IS NOT NULL  
    GROUP BY 
    1,2,3

),  missing_net_arr_base AS (
    SELECT 
 --IDs
    opportunity_snapshot_base.dim_crm_opportunity_id,
    opportunity_snapshot_base.dim_crm_account_id,
    opportunity_snapshot_base.dim_parent_crm_account_id,
    NULL AS dim_crm_touchpoint_id,

  --Dates
    opportunity_snapshot_base.created_date,
    opportunity_snapshot_base.sales_accepted_date,
    opportunity_snapshot_base.pipeline_created_date,
    opportunity_snapshot_base.pipeline_created_fiscal_quarter_name,
    opportunity_snapshot_base.pipeline_created_fiscal_year,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_quarter_normalised,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_year_normalised,
    opportunity_snapshot_base.net_arr_created_date,
    opportunity_snapshot_base.close_date,
    opportunity_snapshot_base.close_fiscal_quarter_name,
    NULL AS bizible_touchpoint_date,
    NULL AS touchpoint_snapshot_date,
    opportunity_snapshot_base.opportunity_snapshot_date,
  
  --Account Info
    opportunity_snapshot_base.account_owner_role,
    opportunity_snapshot_base.parent_crm_account_territory,
    opportunity_snapshot_base.parent_crm_account_sales_segment,
    opportunity_snapshot_base.parent_crm_account_geo,
    opportunity_snapshot_base.parent_crm_account_region,
    opportunity_snapshot_base.parent_crm_account_area,
    opportunity_snapshot_base.account_name,
    opportunity_snapshot_base.parent_crm_account_name,

--Opportunity Dimensions
    opportunity_snapshot_base.opportunity_category,
    opportunity_snapshot_base.subscription_type,
    opportunity_snapshot_base.order_type,
    opportunity_snapshot_base.sales_qualified_source_name,
    opportunity_snapshot_base.stage_name,
    opportunity_snapshot_base.report_segment,
    opportunity_snapshot_base.report_geo,
    opportunity_snapshot_base.report_area,
    opportunity_snapshot_base.parent_crm_account_geo_pubsec_segment,
    opportunity_snapshot_base.report_region,
    opportunity_snapshot_base.report_role_level_1,
    opportunity_snapshot_base.report_role_level_2,
    opportunity_snapshot_base.report_role_level_2_clean,
    opportunity_snapshot_base.report_role_level_3,
    opportunity_snapshot_base.report_role_level_3_clean,
    opportunity_snapshot_base.report_role_level_4,
    opportunity_snapshot_base.report_role_level_5,
    opportunity_snapshot_base.pipe_council_grouping,


--Touchpoint Dimensions
    NULL AS bizible_touchpoint_type,
    NULL AS bizible_integrated_campaign_grouping,
    NULL AS opp_touchpoint_sales_stage,
    'Other' AS bizible_marketing_channel,
    'Other.Removed Touchpoint' AS bizible_marketing_channel_path,
    'Other' AS snapshot_marketing_channel,
    'Other.Removed Touchpoint' AS snapshot_marketing_channel_path,
    'Other.Removed Touchpoint' AS marketing_review_channel_grouping,
    NULL AS bizible_ad_campaign_name,
    NULL AS bizible_form_url,
    NULL AS budget_holder,
    NULL AS campaign_rep_role_name,
    NULL AS campaign_region,
    NULL AS campaign_sub_region,
    NULL AS budgeted_cost,
    NULL AS actual_cost,
    NULL AS keystone_content_name,
    NULL AS keystone_gitlab_epic,
    NULL AS keystone_gtm,
    NULL AS keystone_url_slug,
    NULL AS keystone_type,
    NULL AS utm_campaign,
    NULL AS utm_source,
    NULL AS utm_medium,
    NULL AS utm_content,
    NULL AS utm_budget,
    NULL AS utm_allptnr,
    NULL AS utm_partnerid,
    NULL AS devrel_campaign_type,
    NULL AS devrel_campaign_description,
    NULL AS devrel_campaign_influence_type,
    NULL AS integrated_budget_holder,
    NULL AS sfdc_campaign_type,
    NULL AS gtm_motion,
    NULL AS person_sales_segment,
    NULL AS touchpoint_offer_type,
    NULL AS touchpoint_offer_type_grouped,
    NULL AS series_campaign_id,
    NULL AS series_campaign_name,

  --Metrics
    opportunity_snapshot_base.opp_net_arr,
    missing_net_arr_difference.count_difference AS bizible_count_custom_model,
    NULL AS custom_net_arr_base,
    missing_net_arr_difference.net_arr_difference AS custom_net_arr,
    COALESCE(custom_net_arr,opportunity_snapshot_base.opp_net_arr) AS net_arr,
  --

  --Flags
    opportunity_snapshot_base.is_sao,
    opportunity_snapshot_base.is_won,
    opportunity_snapshot_base.is_web_portal_purchase,
    opportunity_snapshot_base.is_edu_oss,
    opportunity_snapshot_base.is_eligible_created_pipeline_flag,
    opportunity_snapshot_base.is_open,
    opportunity_snapshot_base.is_lost,
    opportunity_snapshot_base.is_closed,
    opportunity_snapshot_base.is_renewal,
    opportunity_snapshot_base.is_refund,
    opportunity_snapshot_base.is_credit_flag,
--    opportunity_snapshot_base.is_eligible_sao_flag, DM: removed during transition to mart_crm_opportunity_daily_snapshot
    opportunity_snapshot_base.is_eligible_open_pipeline_flag,
    opportunity_snapshot_base.is_booked_net_arr_flag,
    opportunity_snapshot_base.is_eligible_age_analysis_flag

    FROM missing_net_arr_difference
    INNER JOIN opportunity_snapshot_base 
      ON missing_net_arr_difference.dim_crm_opportunity_id = opportunity_snapshot_base.dim_crm_opportunity_id 
        AND missing_net_arr_difference.pipeline_created_fiscal_quarter_name = opportunity_snapshot_base.pipeline_created_fiscal_quarter_name

), final AS (
  
    SELECT *
    FROM combined_models
    UNION ALL
    SELECT *
    FROM missing_net_arr_base
)

SELECT *
FROM final
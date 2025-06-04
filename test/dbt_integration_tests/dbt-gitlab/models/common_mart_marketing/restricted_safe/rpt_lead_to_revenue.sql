{{ config(
    materialized="table"
) }}

{{ simple_cte([
    ('mart_crm_person','mart_crm_person'),
    ('dim_crm_person','dim_crm_person'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'), 
    ('map_alternative_lead_demographics','map_alternative_lead_demographics'),
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('mart_crm_account', 'mart_crm_account'),
    ('dim_date','dim_date')
]) }}

, mart_crm_person_with_tp AS (

    SELECT
  --IDs
      mart_crm_person.dim_crm_person_id,
      mart_crm_person.dim_crm_account_id,
      mart_crm_account.dim_parent_crm_account_id,
      dim_crm_person.sfdc_record_id,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.dim_campaign_id,
      mart_crm_person.dim_crm_user_id,
  
  --Person Data
      mart_crm_person.email_hash,
      mart_crm_person.sfdc_record_type,
      mart_crm_person.person_first_country,
      mart_crm_person.email_domain_type,
      mart_crm_person.source_buckets,
      mart_crm_person.true_inquiry_date_pt,
      mart_crm_person.mql_date_first_pt,
      mart_crm_person.mql_date_latest_pt,
      mart_crm_person.accepted_date,
      mart_crm_person.status,
      mart_crm_person.person_role,
      mart_crm_person.lead_source,
      mart_crm_person.is_inquiry,
      mart_crm_person.is_mql,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_territory,
      mart_crm_person.traction_first_response_time,
      mart_crm_person.traction_first_response_time_seconds,
      mart_crm_person.traction_response_time_in_business_hours,
      mart_crm_person.usergem_past_account_id,
      mart_crm_person.usergem_past_account_type,
      mart_crm_person.usergem_past_contact_relationship,
      mart_crm_person.usergem_past_company,
      mart_crm_account.is_first_order_available,
      mart_crm_person.is_bdr_sdr_worked,
      CASE
        WHEN mart_crm_person.is_first_order_person = TRUE 
          THEN '1. New - First Order'
        ELSE '3. Growth'
      END AS person_order_type,
      mart_crm_person.lead_score_classification,
      mart_crm_person.is_defaulted_trial,
      mart_crm_person.is_exclude_from_reporting,

    --MQL and Most Recent Touchpoint info
      mart_crm_person.bizible_mql_touchpoint_id,
      mart_crm_person.bizible_mql_touchpoint_date,
      mart_crm_person.bizible_mql_form_url,
      mart_crm_person.bizible_mql_sfdc_campaign_id,
      mart_crm_person.bizible_mql_ad_campaign_name,
      mart_crm_person.bizible_mql_marketing_channel,
      mart_crm_person.bizible_mql_marketing_channel_path,
      mart_crm_person.bizible_most_recent_touchpoint_id,
      mart_crm_person.bizible_most_recent_touchpoint_date,
      mart_crm_person.bizible_most_recent_form_url,
      mart_crm_person.bizible_most_recent_sfdc_campaign_id,
      mart_crm_person.bizible_most_recent_ad_campaign_name,
      mart_crm_person.bizible_most_recent_marketing_channel,
      mart_crm_person.bizible_most_recent_marketing_channel_path,

  --Account Data
      mart_crm_account.crm_account_name,
      mart_crm_account.crm_account_type,
      mart_crm_account.parent_crm_account_name,
      mart_crm_account.parent_crm_account_lam,
      mart_crm_account.parent_crm_account_lam_dev_count,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom,map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,

  -- Account Groove Data
      mart_crm_account.groove_notes AS account_groove_notes,
      mart_crm_account.groove_engagement_status AS account_groove_engagement_status,
      mart_crm_account.groove_inferred_status AS account_groove_inferred_status,

  --Touchpoint Data
      'Person Touchpoint' AS touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_touchpoint_position,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_ad_group_name,
      mart_crm_touchpoint.bizible_salesforce_campaign,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      mart_crm_touchpoint.bizible_marketing_channel,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.marketing_review_channel_grouping,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.bizible_form_page_utm_content,
      mart_crm_touchpoint.bizible_form_page_utm_budget,
      mart_crm_touchpoint.bizible_form_page_utm_allptnr,
      mart_crm_touchpoint.bizible_form_page_utm_partnerid,
      mart_crm_touchpoint.bizible_landing_page_utm_content,
      mart_crm_touchpoint.bizible_landing_page_utm_budget,
      mart_crm_touchpoint.bizible_landing_page_utm_allptnr,
      mart_crm_touchpoint.bizible_landing_page_utm_partnerid,
      mart_crm_touchpoint.utm_campaign_date,
      mart_crm_touchpoint.utm_campaign_region,
      mart_crm_touchpoint.utm_campaign_budget,
      mart_crm_touchpoint.utm_campaign_type,
      mart_crm_touchpoint.utm_campaign_gtm,
      mart_crm_touchpoint.utm_campaign_language,
      mart_crm_touchpoint.utm_campaign_name,
      mart_crm_touchpoint.utm_campaign_agency,
      mart_crm_touchpoint.utm_content_offer,
      mart_crm_touchpoint.utm_content_asset_type,
      mart_crm_touchpoint.utm_content_industry,
      mart_crm_touchpoint.campaign_rep_role_name,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.pipe_name,
      mart_crm_touchpoint.is_dg_influenced,
      mart_crm_touchpoint.is_dg_sourced,
      mart_crm_touchpoint.bizible_count_first_touch,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.bizible_count_u_shaped,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.is_fmm_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum,
      mart_crm_touchpoint.touchpoint_offer_type_grouped,
      mart_crm_touchpoint.touchpoint_offer_type   
    FROM mart_crm_person
    INNER JOIN dim_crm_person
      ON mart_crm_person.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    FULL JOIN mart_crm_account
      ON mart_crm_person.dim_crm_account_id = mart_crm_account.dim_crm_account_id
    LEFT JOIN mart_crm_touchpoint
      ON mart_crm_touchpoint.email_hash = mart_crm_person.email_hash
    LEFT JOIN map_alternative_lead_demographics
      ON mart_crm_person.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
  
  ), pre_aggregated_data AS (

    SELECT
      mart_crm_attribution_touchpoint.dim_crm_opportunity_id,
      SUM(mart_crm_attribution_touchpoint.bizible_count_first_touch) AS sum_first_touch,
      SUM(mart_crm_attribution_touchpoint.bizible_count_u_shaped) AS sum_u_shaped,
      SUM(mart_crm_attribution_touchpoint.bizible_count_w_shaped) AS sum_w_shaped,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS sum_custom_model,
      SUM(mart_crm_attribution_touchpoint.l_weight) AS sum_l_weight,
      SUM(mart_crm_attribution_touchpoint.first_net_arr) AS sum_first_net_arr,
      SUM(mart_crm_attribution_touchpoint.u_net_arr) AS sum_u_net_arr,
      SUM(mart_crm_attribution_touchpoint.w_net_arr) AS sum_w_net_arr,
      SUM(mart_crm_attribution_touchpoint.full_net_arr) AS sum_full_net_arr,
      SUM(mart_crm_attribution_touchpoint.custom_net_arr) AS sum_custom_net_arr,
      SUM(mart_crm_attribution_touchpoint.linear_net_arr) AS sum_linear_net_arr
    FROM mart_crm_attribution_touchpoint
    GROUP BY mart_crm_attribution_touchpoint.dim_crm_opportunity_id
 
  ), opp_base_with_batp AS (
    
    SELECT
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      mart_crm_opportunity.*,

    --Account Data
      mart_crm_account.parent_crm_account_lam,
      mart_crm_account.crm_account_type,

    -- Account Groove Data
      mart_crm_account.groove_notes AS account_groove_notes,
      mart_crm_account.groove_engagement_status AS account_groove_engagement_status,
      mart_crm_account.groove_inferred_status AS account_groove_inferred_status,
    
    -- Touchpoint Data
      'Attribution Touchpoint' AS touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.bizible_ad_group_name,
      mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.marketing_review_channel_grouping,
      mart_crm_attribution_touchpoint.bizible_medium,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_content,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_budget,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_allptnr,
      mart_crm_attribution_touchpoint.bizible_form_page_utm_partnerid,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_content,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_budget,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_allptnr,
      mart_crm_attribution_touchpoint.bizible_landing_page_utm_partnerid,
      mart_crm_attribution_touchpoint.utm_campaign_date,
      mart_crm_attribution_touchpoint.utm_campaign_region,
      mart_crm_attribution_touchpoint.utm_campaign_budget,
      mart_crm_attribution_touchpoint.utm_campaign_type,
      mart_crm_attribution_touchpoint.utm_campaign_gtm,
      mart_crm_attribution_touchpoint.utm_campaign_language,
      mart_crm_attribution_touchpoint.utm_campaign_name,
      mart_crm_attribution_touchpoint.utm_campaign_agency,
      mart_crm_attribution_touchpoint.utm_content_offer,
      mart_crm_attribution_touchpoint.utm_content_asset_type,
      mart_crm_attribution_touchpoint.utm_content_industry,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.campaign_rep_role_name,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.pipe_name,
      mart_crm_attribution_touchpoint.is_dg_influenced,
      mart_crm_attribution_touchpoint.is_dg_sourced,
      mart_crm_attribution_touchpoint.is_mgp_channel_based,
      mart_crm_attribution_touchpoint.is_mgp_opportunity,
      mart_crm_attribution_touchpoint.gitlab_model_weight,
      mart_crm_attribution_touchpoint.time_decay_model_weight,
      mart_crm_attribution_touchpoint.data_driven_model_weight,
      mart_crm_attribution_touchpoint.bizible_count_first_touch,
      mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_attribution_touchpoint.bizible_count_u_shaped,
      mart_crm_attribution_touchpoint.bizible_count_w_shaped,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_u_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_w_shaped,
      mart_crm_attribution_touchpoint.bizible_weight_full_path,
      mart_crm_attribution_touchpoint.bizible_weight_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_first_touch,
      mart_crm_attribution_touchpoint.is_fmm_influenced,
      mart_crm_attribution_touchpoint.is_fmm_sourced,
      mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
      mart_crm_attribution_touchpoint.touchpoint_offer_type,  
      mart_crm_attribution_touchpoint.touchpoint_sales_stage,
      CASE
          WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT null 
            THEN mart_crm_opportunity.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id, 
      pre_aggregated_data.sum_first_touch AS first_opp_created,
      pre_aggregated_data.sum_u_shaped AS u_shaped_opp_created,
      pre_aggregated_data.sum_w_shaped AS w_shaped_opp_created,
      pre_aggregated_data.sum_first_touch AS full_shaped_opp_created,
      pre_aggregated_data.sum_custom_model AS custom_opp_created,
      pre_aggregated_data.sum_l_weight AS linear_opp_created,
      pre_aggregated_data.sum_first_net_arr AS first_net_arr,
      pre_aggregated_data.sum_u_net_arr AS u_net_arr,
      pre_aggregated_data.sum_w_net_arr AS w_net_arr,
      pre_aggregated_data.sum_full_net_arr AS full_net_arr,
      pre_aggregated_data.sum_custom_net_arr AS custom_net_arr,
      pre_aggregated_data.sum_linear_net_arr AS linear_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_first_touch::NUMBER(38,0), 0::NUMBER(38,0)) AS first_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_u_shaped::NUMBER(38,0), 0::NUMBER(38,0)) AS u_shaped_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_w_shaped::NUMBER(38,0), 0::NUMBER(38,0)) AS w_shaped_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_first_touch::NUMBER(38,0), 0::NUMBER(38,0)) AS full_shaped_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_custom_model::NUMBER(38,0), 0::NUMBER(38,0)) AS custom_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_l_weight::NUMBER(38,0), 0::NUMBER(38,0)) AS linear_sao,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_first_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_first_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_u_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_u_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_w_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_w_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_full_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_full_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_custom_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_custom_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_pipeline_created=TRUE, pre_aggregated_data.sum_linear_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS pipeline_linear_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_first_touch::NUMBER(38,0), 0::NUMBER(38,0)) AS won_first,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_u_shaped::NUMBER(38,0), 0::NUMBER(38,0)) AS won_u,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_w_shaped::NUMBER(38,0), 0::NUMBER(38,0)) AS won_w,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_first_touch::NUMBER(38,0), 0::NUMBER(38,0)) AS won_full,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_custom_model::NUMBER(38,0), 0::NUMBER(38,0)) AS won_custom,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_l_weight::NUMBER(38,0), 0::NUMBER(38,0)) AS won_linear,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_first_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_first_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_u_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_u_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_w_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_w_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_full_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_full_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_custom_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_custom_net_arr,
      IFF(mart_crm_opportunity.is_net_arr_closed_deal=TRUE, pre_aggregated_data.sum_linear_net_arr::NUMBER(38,0), 0::NUMBER(38,0)) AS won_linear_net_arr
    FROM mart_crm_opportunity
    LEFT JOIN mart_crm_attribution_touchpoint
      ON mart_crm_opportunity.dim_crm_opportunity_id=mart_crm_attribution_touchpoint.dim_crm_opportunity_id
    LEFT JOIN pre_aggregated_data
      ON mart_crm_opportunity.dim_crm_opportunity_id = pre_aggregated_data.dim_crm_opportunity_id
    FULL JOIN mart_crm_account
      ON mart_crm_opportunity.dim_crm_account_id=mart_crm_account.dim_crm_account_id
    WHERE mart_crm_opportunity.created_date >= '2021-02-01'
      OR mart_crm_opportunity.created_date IS NULL
    
), cohort_base_combined AS (
  
    SELECT
   --IDs
      mart_crm_person_with_tp.dim_crm_person_id,
      COALESCE (mart_crm_person_with_tp.dim_crm_account_id,opp_base_with_batp.dim_crm_account_id) AS dim_crm_account_id,
      COALESCE (mart_crm_person_with_tp.dim_parent_crm_account_id,opp_base_with_batp.dim_parent_crm_account_id) AS dim_parent_crm_account_id,
      mart_crm_person_with_tp.sfdc_record_id,
      COALESCE (mart_crm_person_with_tp.dim_crm_touchpoint_id,opp_base_with_batp.dim_crm_touchpoint_id) AS dim_crm_touchpoint_id, 
      mart_crm_person_with_tp.dim_crm_touchpoint_id AS dim_crm_btp_touchpoint_id,
      opp_base_with_batp.dim_crm_touchpoint_id AS dim_crm_batp_touchpoint_id,
      opp_base_with_batp.dim_crm_opportunity_id,
      COALESCE (mart_crm_person_with_tp.dim_campaign_id,opp_base_with_batp.dim_campaign_id) AS dim_campaign_id, 
      mart_crm_person_with_tp.dim_crm_user_id,
      opp_base_with_batp.dim_crm_user_id AS opp_dim_crm_user_id,
      opp_base_with_batp.contract_reset_opportunity_id,
  
  --Person Data
      mart_crm_person_with_tp.email_hash,
      mart_crm_person_with_tp.email_domain_type,
      mart_crm_person_with_tp.sfdc_record_type,
      mart_crm_person_with_tp.person_role,
      mart_crm_person_with_tp.person_first_country,
      mart_crm_person_with_tp.source_buckets,
      mart_crm_person_with_tp.true_inquiry_date_pt,
      mart_crm_person_with_tp.mql_date_first_pt,
      mart_crm_person_with_tp.mql_date_latest_pt,
      mart_crm_person_with_tp.accepted_date,
      mart_crm_person_with_tp.status,
      mart_crm_person_with_tp.lead_source,
      mart_crm_person_with_tp.is_inquiry,
      mart_crm_person_with_tp.is_mql,
      mart_crm_person_with_tp.account_demographics_sales_segment,
      mart_crm_person_with_tp.account_demographics_region,
      mart_crm_person_with_tp.account_demographics_geo,
      mart_crm_person_with_tp.account_demographics_area,
      mart_crm_person_with_tp.account_demographics_upa_country,
      mart_crm_person_with_tp.account_demographics_territory,
      mart_crm_person_with_tp.is_first_order_available,
      mart_crm_person_with_tp.is_bdr_sdr_worked,
      mart_crm_person_with_tp.person_order_type,
      mart_crm_person_with_tp.employee_count_segment_custom,
      mart_crm_person_with_tp.employee_bucket_segment_custom,
      mart_crm_person_with_tp.inferred_employee_segment,
      mart_crm_person_with_tp.geo_custom,
      mart_crm_person_with_tp.inferred_geo,
      mart_crm_person_with_tp.traction_first_response_time,
      mart_crm_person_with_tp.traction_first_response_time_seconds,
      mart_crm_person_with_tp.traction_response_time_in_business_hours,
      mart_crm_person_with_tp.usergem_past_account_id,
      mart_crm_person_with_tp.usergem_past_account_type,
      mart_crm_person_with_tp.usergem_past_contact_relationship,
      mart_crm_person_with_tp.usergem_past_company,
      mart_crm_person_with_tp.lead_score_classification,
      mart_crm_person_with_tp.is_defaulted_trial,

  --MQL and Most Recent Touchpoint info
      mart_crm_person_with_tp.bizible_mql_touchpoint_id,
      mart_crm_person_with_tp.bizible_mql_touchpoint_date,
      mart_crm_person_with_tp.bizible_mql_form_url,
      mart_crm_person_with_tp.bizible_mql_sfdc_campaign_id,
      mart_crm_person_with_tp.bizible_mql_ad_campaign_name,
      mart_crm_person_with_tp.bizible_mql_marketing_channel,
      mart_crm_person_with_tp.bizible_mql_marketing_channel_path,
      mart_crm_person_with_tp.bizible_most_recent_touchpoint_id,
      mart_crm_person_with_tp.bizible_most_recent_touchpoint_date,
      mart_crm_person_with_tp.bizible_most_recent_form_url,
      mart_crm_person_with_tp.bizible_most_recent_sfdc_campaign_id,
      mart_crm_person_with_tp.bizible_most_recent_ad_campaign_name,
      mart_crm_person_with_tp.bizible_most_recent_marketing_channel,
      mart_crm_person_with_tp.bizible_most_recent_marketing_channel_path,
  
  --Opp Data
      opp_base_with_batp.opportunity_name,
      opp_base_with_batp.order_type AS opp_order_type,
      opp_base_with_batp.sdr_or_bdr,
      opp_base_with_batp.sales_qualified_source_name,
      opp_base_with_batp.deal_path_name,
      opp_base_with_batp.subscription_type,
      opp_base_with_batp.sales_accepted_date,
      opp_base_with_batp.created_date AS opp_created_date,
      opp_base_with_batp.close_date,
      opp_base_with_batp.lead_source AS opp_lead_source,
      opp_base_with_batp.source_buckets AS opp_source_buckets,
      opp_base_with_batp.is_won,
      opp_base_with_batp.valid_deal_count,
      opp_base_with_batp.is_sao,
      opp_base_with_batp.new_logo_count,
      opp_base_with_batp.net_arr,
      opp_base_with_batp.net_arr_stage_1,
      opp_base_with_batp.xdr_net_arr_stage_1,
      opp_base_with_batp.xdr_net_arr_stage_3,
      opp_base_with_batp.is_net_arr_closed_deal,
      opp_base_with_batp.is_net_arr_pipeline_created,
      opp_base_with_batp.is_eligible_age_analysis,
      opp_base_with_batp.parent_crm_account_sales_segment AS opp_account_demographics_sales_segment,
      opp_base_with_batp.parent_crm_account_region AS opp_account_demographics_region,
      opp_base_with_batp.parent_crm_account_geo AS opp_account_demographics_geo,
      opp_base_with_batp.parent_crm_account_territory AS opp_account_demographics_territory,
      opp_base_with_batp.parent_crm_account_area AS opp_account_demographics_area,
      opp_base_with_batp.report_segment,
      opp_base_with_batp.report_region,
      opp_base_with_batp.report_area,
      opp_base_with_batp.report_geo,
      opp_base_with_batp.report_role_name,
      opp_base_with_batp.report_role_level_1,
      opp_base_with_batp.report_role_level_2,
      opp_base_with_batp.report_role_level_3,
      opp_base_with_batp.report_role_level_4,
      opp_base_with_batp.report_role_level_5,
      opp_base_with_batp.product_category,
      opp_base_with_batp.is_renewal,
      opp_base_with_batp.is_eligible_open_pipeline,
      opp_base_with_batp.pipeline_created_date,
      opp_base_with_batp.opportunity_category,
      opp_base_with_batp.stage_name,
      opp_base_with_batp.is_jihu_account,
      opp_base_with_batp.is_edu_oss,
      opp_base_with_batp.stage_1_discovery_date,
      opp_base_with_batp.stage_3_technical_evaluation_date,

  --Account Data
      COALESCE(mart_crm_person_with_tp.crm_account_name,opp_base_with_batp.crm_account_name) AS crm_account_name,
      COALESCE(mart_crm_person_with_tp.crm_account_type,opp_base_with_batp.crm_account_type) AS crm_account_type,
      COALESCE(mart_crm_person_with_tp.parent_crm_account_name,opp_base_with_batp.parent_crm_account_name) AS parent_crm_account_name,
      COALESCE(mart_crm_person_with_tp.parent_crm_account_lam,opp_base_with_batp.parent_crm_account_lam) AS parent_crm_account_lam,
      COALESCE(mart_crm_person_with_tp.parent_crm_account_lam_dev_count,opp_base_with_batp.parent_crm_account_lam_dev_count) AS parent_crm_account_lam_dev_count,
      opp_base_with_batp.parent_crm_account_upa_country,
      opp_base_with_batp.parent_crm_account_territory,

  -- Account Groove Data
      COALESCE(mart_crm_person_with_tp.account_groove_notes,opp_base_with_batp.account_groove_notes) AS account_groove_notes,
      COALESCE(mart_crm_person_with_tp.account_groove_engagement_status,opp_base_with_batp.account_groove_engagement_status) AS account_groove_engagement_status,
      COALESCE(mart_crm_person_with_tp.account_groove_inferred_status,opp_base_with_batp.account_groove_inferred_status) AS account_groove_inferred_status,
  
  --Touchpoint Data
      COALESCE(mart_crm_person_with_tp.touchpoint_offer_type_grouped,opp_base_with_batp.touchpoint_offer_type_grouped) AS touchpoint_offer_type_grouped, 
      COALESCE(mart_crm_person_with_tp.touchpoint_offer_type,opp_base_with_batp.touchpoint_offer_type) AS touchpoint_offer_type,
      opp_base_with_batp.touchpoint_offer_type_grouped AS opp_touchpoint_offer_type_grouped,
      opp_base_with_batp.touchpoint_offer_type AS opp_touchpoint_offer_type, 
      opp_base_with_batp.touchpoint_sales_stage AS opp_touchpoint_sales_stage,
      COALESCE(mart_crm_person_with_tp.bizible_touchpoint_date,opp_base_with_batp.bizible_touchpoint_date) AS bizible_touchpoint_date, 
      COALESCE(mart_crm_person_with_tp.campaign_rep_role_name,opp_base_with_batp.campaign_rep_role_name) AS campaign_rep_role_name, 
      COALESCE(mart_crm_person_with_tp.bizible_touchpoint_position,opp_base_with_batp.bizible_touchpoint_position) AS bizible_touchpoint_position, 
      COALESCE(mart_crm_person_with_tp.bizible_touchpoint_source,opp_base_with_batp.bizible_touchpoint_source) AS bizible_touchpoint_source, 
      COALESCE(mart_crm_person_with_tp.bizible_touchpoint_type,opp_base_with_batp.bizible_touchpoint_type) AS bizible_touchpoint_type, 
      COALESCE(mart_crm_person_with_tp.bizible_ad_campaign_name,opp_base_with_batp.bizible_ad_campaign_name) AS bizible_ad_campaign_name, 
      COALESCE(mart_crm_person_with_tp.bizible_ad_group_name,opp_base_with_batp.bizible_ad_group_name) AS bizible_ad_group_name, 
      COALESCE(mart_crm_person_with_tp.bizible_salesforce_campaign,opp_base_with_batp.bizible_salesforce_campaign) AS bizible_salesforce_campaign, 
      COALESCE(mart_crm_person_with_tp.bizible_form_url,opp_base_with_batp.bizible_form_url) AS bizible_form_url, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page,opp_base_with_batp.bizible_landing_page) AS bizible_landing_page, 
      COALESCE(mart_crm_person_with_tp.bizible_form_url_raw,opp_base_with_batp.bizible_form_url_raw) AS bizible_form_url_raw, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page_raw,opp_base_with_batp.bizible_landing_page_raw) AS bizible_landing_page_raw, 
      COALESCE(mart_crm_person_with_tp.bizible_marketing_channel,opp_base_with_batp.bizible_marketing_channel) AS bizible_marketing_channel, 
      opp_base_with_batp.bizible_marketing_channel AS opp_bizible_marketing_channel,
      COALESCE(mart_crm_person_with_tp.bizible_marketing_channel_path,opp_base_with_batp.bizible_marketing_channel_path) AS bizible_marketing_channel_path, 
      opp_base_with_batp.bizible_marketing_channel_path AS opp_bizible_marketing_channel_path,
      COALESCE(mart_crm_person_with_tp.bizible_medium,opp_base_with_batp.bizible_medium) AS bizible_medium, 
      COALESCE(mart_crm_person_with_tp.bizible_referrer_page,opp_base_with_batp.bizible_referrer_page) AS bizible_referrer_page, 
      COALESCE(mart_crm_person_with_tp.bizible_referrer_page_raw,opp_base_with_batp.bizible_referrer_page_raw) AS bizible_referrer_page_raw, 
      COALESCE(mart_crm_person_with_tp.bizible_integrated_campaign_grouping,opp_base_with_batp.bizible_integrated_campaign_grouping) AS bizible_integrated_campaign_grouping, 
      COALESCE(mart_crm_person_with_tp.touchpoint_segment,opp_base_with_batp.touchpoint_segment) AS touchpoint_segment, 
      COALESCE(mart_crm_person_with_tp.gtm_motion,opp_base_with_batp.gtm_motion) AS gtm_motion, 
      COALESCE(mart_crm_person_with_tp.pipe_name,opp_base_with_batp.pipe_name) AS pipe_name, 
      COALESCE(mart_crm_person_with_tp.is_dg_influenced,opp_base_with_batp.is_dg_influenced) AS is_dg_influenced, 
      COALESCE(mart_crm_person_with_tp.is_dg_sourced,opp_base_with_batp.is_dg_sourced) AS is_dg_sourced,
      opp_base_with_batp.is_mgp_channel_based,
      opp_base_with_batp.is_mgp_opportunity,
      opp_base_with_batp.gitlab_model_weight,
      opp_base_with_batp.time_decay_model_weight,
      opp_base_with_batp.data_driven_model_weight,
      COALESCE(mart_crm_person_with_tp.bizible_count_first_touch,opp_base_with_batp.bizible_count_first_touch) AS bizible_count_first_touch, 
      COALESCE(mart_crm_person_with_tp.bizible_count_lead_creation_touch,opp_base_with_batp.bizible_count_lead_creation_touch) AS bizible_count_lead_creation_touch, 
      COALESCE(mart_crm_person_with_tp.bizible_count_u_shaped,opp_base_with_batp.bizible_count_u_shaped) AS bizible_count_u_shaped, 
      COALESCE(mart_crm_person_with_tp.is_fmm_influenced,opp_base_with_batp.is_fmm_influenced) AS is_fmm_influenced, 
      COALESCE(mart_crm_person_with_tp.is_fmm_sourced,opp_base_with_batp.is_fmm_sourced) AS is_fmm_sourced,
      COALESCE(mart_crm_person_with_tp.bizible_form_page_utm_content,opp_base_with_batp.bizible_form_page_utm_content) AS bizible_form_page_utm_content, 
      COALESCE(mart_crm_person_with_tp.bizible_form_page_utm_budget,opp_base_with_batp.bizible_form_page_utm_budget) AS bizible_form_page_utm_budget, 
      COALESCE(mart_crm_person_with_tp.bizible_form_page_utm_allptnr,opp_base_with_batp.bizible_form_page_utm_allptnr) AS bizible_form_page_utm_allptnr, 
      COALESCE(mart_crm_person_with_tp.bizible_form_page_utm_partnerid,opp_base_with_batp.bizible_form_page_utm_partnerid) AS bizible_form_page_utm_partnerid, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page_utm_content,opp_base_with_batp.bizible_landing_page_utm_content) AS bizible_landing_page_utm_content, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page_utm_budget,opp_base_with_batp.bizible_landing_page_utm_budget) AS bizible_landing_page_utm_budget, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page_utm_allptnr,opp_base_with_batp.bizible_landing_page_utm_allptnr) AS bizible_landing_page_utm_allptnr, 
      COALESCE(mart_crm_person_with_tp.bizible_landing_page_utm_partnerid,opp_base_with_batp.bizible_landing_page_utm_partnerid) AS bizible_landing_page_utm_partnerid, 
      COALESCE(mart_crm_person_with_tp.utm_campaign_date,opp_base_with_batp.utm_campaign_date) AS bizible_utm_campaign_date,
      COALESCE(mart_crm_person_with_tp.utm_campaign_region,opp_base_with_batp.utm_campaign_region) AS bizible_utm_campaign_region,
      COALESCE(mart_crm_person_with_tp.utm_campaign_budget,opp_base_with_batp.utm_campaign_budget) AS bizible_utm_campaign_budget,
      COALESCE(mart_crm_person_with_tp.utm_campaign_type,opp_base_with_batp.utm_campaign_type) AS bizible_utm_campaign_type,
      COALESCE(mart_crm_person_with_tp.utm_campaign_gtm,opp_base_with_batp.utm_campaign_gtm) AS bizible_utm_campaign_gtm,
      COALESCE(mart_crm_person_with_tp.utm_campaign_language,opp_base_with_batp.utm_campaign_language) AS bizible_utm_campaign_language,
      COALESCE(mart_crm_person_with_tp.utm_campaign_name,opp_base_with_batp.utm_campaign_name) AS bizible_utm_campaign_name,
      COALESCE(mart_crm_person_with_tp.utm_campaign_agency,opp_base_with_batp.utm_campaign_agency) AS bizible_utm_campaign_agency,
      COALESCE(mart_crm_person_with_tp.utm_content_offer,opp_base_with_batp.utm_content_offer) AS bizible_utm_content_offer,
      COALESCE(mart_crm_person_with_tp.utm_content_asset_type,opp_base_with_batp.utm_content_asset_type) AS bizible_utm_content_asset_type,
      COALESCE(mart_crm_person_with_tp.utm_content_industry,opp_base_with_batp.utm_content_industry) AS bizible_utm_content_industry,
      mart_crm_person_with_tp.new_lead_created_sum,
      mart_crm_person_with_tp.count_true_inquiry,
      mart_crm_person_with_tp.inquiry_sum, 
      mart_crm_person_with_tp.mql_sum,
      mart_crm_person_with_tp.accepted_sum,
      mart_crm_person_with_tp.new_mql_sum,
      mart_crm_person_with_tp.new_accepted_sum,
      opp_base_with_batp.bizible_count_w_shaped,
      opp_base_with_batp.bizible_count_custom_model,
      opp_base_with_batp.bizible_weight_custom_model,
      opp_base_with_batp.bizible_weight_u_shaped,
      opp_base_with_batp.bizible_weight_w_shaped,
      opp_base_with_batp.bizible_weight_full_path,
      opp_base_with_batp.bizible_weight_first_touch,
      opp_base_with_batp.influenced_opportunity_id,
      opp_base_with_batp.first_opp_created,
      opp_base_with_batp.u_shaped_opp_created,
      opp_base_with_batp.w_shaped_opp_created,
      opp_base_with_batp.full_shaped_opp_created,
      opp_base_with_batp.custom_opp_created,
      opp_base_with_batp.linear_opp_created,
      opp_base_with_batp.first_net_arr,
      opp_base_with_batp.u_net_arr,
      opp_base_with_batp.w_net_arr,
      opp_base_with_batp.full_net_arr,
      opp_base_with_batp.custom_net_arr,
      opp_base_with_batp.linear_net_arr,
      opp_base_with_batp.first_sao,
      opp_base_with_batp.u_shaped_sao,
      opp_base_with_batp.w_shaped_sao,
      opp_base_with_batp.full_shaped_sao,
      opp_base_with_batp.custom_sao,
      opp_base_with_batp.linear_sao,
      opp_base_with_batp.pipeline_first_net_arr,
      opp_base_with_batp.pipeline_u_net_arr,
      opp_base_with_batp.pipeline_w_net_arr,
      opp_base_with_batp.pipeline_full_net_arr,
      opp_base_with_batp.pipeline_custom_net_arr,
      opp_base_with_batp.pipeline_linear_net_arr,
      opp_base_with_batp.won_first,
      opp_base_with_batp.won_u,
      opp_base_with_batp.won_w,
      opp_base_with_batp.won_full,
      opp_base_with_batp.won_custom,
      opp_base_with_batp.won_linear,
      opp_base_with_batp.won_first_net_arr,
      opp_base_with_batp.won_u_net_arr,
      opp_base_with_batp.won_w_net_arr,
      opp_base_with_batp.won_full_net_arr,
      opp_base_with_batp.won_custom_net_arr,
      opp_base_with_batp.won_linear_net_arr
  FROM mart_crm_person_with_tp
  FULL JOIN opp_base_with_batp
    ON mart_crm_person_with_tp.dim_crm_account_id=opp_base_with_batp.dim_crm_account_id

), intermediate AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['cohort_base_combined.dim_crm_person_id','cohort_base_combined.dim_crm_btp_touchpoint_id','cohort_base_combined.dim_crm_batp_touchpoint_id','cohort_base_combined.dim_crm_opportunity_id']) }}
                                                 AS lead_to_revenue_id,
    cohort_base_combined.*,
    --inquiry_date fields
    inquiry_date.fiscal_year                     AS inquiry_date_range_year,
    inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
    DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
    inquiry_date.first_day_of_week               AS inquiry_date_range_week,
    inquiry_date.date_id                         AS inquiry_date_range_id,
  
    --mql_date fields
    mql_date.fiscal_year                     AS mql_date_range_year,
    mql_date.fiscal_quarter_name_fy          AS mql_date_range_quarter,
    DATE_TRUNC(month, mql_date.date_actual)  AS mql_date_range_month,
    mql_date.first_day_of_week               AS mql_date_range_week,
    mql_date.date_id                         AS mql_date_range_id,
  
    --opp_create_date fields
    opp_create_date.fiscal_year                     AS opportunity_created_date_range_year,
    opp_create_date.fiscal_quarter_name_fy          AS opportunity_created_date_range_quarter,
    DATE_TRUNC(month, opp_create_date.date_actual)  AS opportunity_created_date_range_month,
    opp_create_date.first_day_of_week               AS opportunity_created_date_range_week,
    opp_create_date.date_id                         AS opportunity_created_date_range_id,
  
    --sao_date fields
    sao_date.fiscal_year                     AS sao_date_range_year,
    sao_date.fiscal_quarter_name_fy          AS sao_date_range_quarter,
    DATE_TRUNC(month, sao_date.date_actual)  AS sao_date_range_month,
    sao_date.first_day_of_week               AS sao_date_range_week,
    sao_date.date_id                         AS sao_date_range_id,

    --stage_1_date fields
    stage_1_date.fiscal_year                     AS stage_1_date_range_year,
    stage_1_date.fiscal_quarter_name_fy          AS stage_1_date_range_quarter,
    DATE_TRUNC(month, stage_1_date.date_actual)  AS stage_1_date_range_month,
    stage_1_date.first_day_of_week               AS stage_1_date_range_week,
    stage_1_date.date_id                         AS stage_1_date_range_id,

    --stage_3_date fields
    stage_3_date.fiscal_year                     AS stage_3_date_range_year,
    stage_3_date.fiscal_quarter_name_fy          AS stage_3_date_range_quarter,
    DATE_TRUNC(month, stage_3_date.date_actual)  AS stage_3_date_range_month,
    stage_3_date.first_day_of_week               AS stage_3_date_range_week,
    stage_3_date.date_id                         AS stage_3_date_range_id,
  
    --closed_date fields
    closed_date.fiscal_year                     AS closed_date_range_year,
    closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
    DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
    closed_date.first_day_of_week               AS closed_date_range_week,
    closed_date.date_id                         AS closed_date_range_id,

    --touchpoint_date fields
    touchpoint_date.fiscal_year                     AS touchpoint_date_range_year,
    touchpoint_date.fiscal_quarter_name_fy          AS touchpoint_date_range_quarter,
    DATE_TRUNC(month, touchpoint_date.date_actual)  AS touchpoint_date_range_month,
    touchpoint_date.first_day_of_week               AS touchpoint_date_range_week,
    touchpoint_date.date_id                         AS touchpoint_date_range_id
  FROM cohort_base_combined
  LEFT JOIN dim_date AS inquiry_date
    ON cohort_base_combined.true_inquiry_date_pt = inquiry_date.date_day
  LEFT JOIN dim_date AS mql_date
    ON cohort_base_combined.mql_date_latest_pt = mql_date.date_day
  LEFT JOIN dim_date AS opp_create_date
    ON cohort_base_combined.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date AS sao_date
    ON cohort_base_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date AS stage_1_date
    ON cohort_base_combined.stage_1_discovery_date = stage_1_date.date_day
  LEFT JOIN dim_date AS stage_3_date
    ON cohort_base_combined.stage_3_technical_evaluation_date = stage_3_date.date_day
  LEFT JOIN dim_date AS closed_date
    ON cohort_base_combined.close_date = closed_date.date_day
  LEFT JOIN dim_date AS touchpoint_date
    ON cohort_base_combined.bizible_touchpoint_date = touchpoint_date.date_day
  WHERE cohort_base_combined.dim_crm_person_id IS NOT NULL
    OR cohort_base_combined.dim_crm_opportunity_id IS NOT NULL

)

SELECT *
FROM intermediate
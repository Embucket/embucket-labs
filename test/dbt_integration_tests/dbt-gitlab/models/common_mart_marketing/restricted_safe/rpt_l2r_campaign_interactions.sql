{{ simple_cte([
    ('mart_crm_person','mart_crm_person'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'), 
    ('mart_crm_touchpoint', 'mart_crm_touchpoint'),
    ('map_alternative_lead_demographics','map_alternative_lead_demographics'),
    ('mart_crm_attribution_touchpoint','mart_crm_attribution_touchpoint'),
    ('dim_crm_account', 'dim_crm_account'),
    ('dim_date','dim_date'),
    ('fct_campaign','fct_campaign'),
    ('dim_campaign', 'dim_campaign'),
    ('dim_crm_user', 'dim_crm_user'),
    ('sfdc_lead_history', 'sfdc_lead_history_source'),
    ('sfdc_contact_history', 'sfdc_contact_history_source')
]) }}

, mart_crm_person_with_tp AS (

    SELECT
  --IDs
      mart_crm_touchpoint.dim_crm_person_id,
      mart_crm_touchpoint.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      mart_crm_person.dim_crm_user_id,
      mart_crm_person.crm_partner_id,
      mart_crm_person.partner_prospect_id,
      mart_crm_person.sfdc_record_id,
      mart_crm_touchpoint.dim_crm_touchpoint_id,
      mart_crm_touchpoint.dim_campaign_id,

  
  --Person Data
      mart_crm_person.email_hash,
      mart_crm_person.email_domain,
      mart_crm_person.was_converted_lead,
      mart_crm_person.email_domain_type,
      mart_crm_person.is_valuable_signup,
      mart_crm_person.status AS crm_person_status,
      mart_crm_person.lead_source,
      mart_crm_person.source_buckets,
      mart_crm_person.is_mql,
      mart_crm_person.is_inquiry,
      mart_crm_person.is_lead_source_trial,
      mart_crm_person.account_demographics_sales_segment_grouped,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_segment_region_grouped,
      mart_crm_person.zoominfo_company_employee_count,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_territory,
      mart_crm_person.person_first_country,
      mart_crm_person.partner_prospect_status,
      mart_crm_person.prospect_share_status,
      dim_crm_account.is_first_order_available,
      mart_crm_person.sales_segment_name AS person_sales_segment_name,
      mart_crm_person.sales_segment_grouped AS person_sales_segment_grouped,
      mart_crm_person.person_score,
      mart_crm_person.behavior_score,
      mart_crm_person.employee_bucket,
      mart_crm_person.leandata_matched_account_sales_Segment,
      mart_crm_person.sfdc_record_type,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom, 
      map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,
      CASE
          WHEN mart_crm_person.is_first_order_person = TRUE 
            THEN '1. New - First Order'
          ELSE '3. Growth'
      END AS person_order_type,
      last_utm_campaign,
      last_utm_content,
      lead_score_classification,
      is_defaulted_trial,
      is_exclude_from_reporting,

  --Person Dates
      mart_crm_person.true_inquiry_date_pt,
      mart_crm_person.mql_date_latest_pt,
      mart_crm_person.legacy_mql_date_first_pt,
      mart_crm_person.mql_sfdc_date_pt,
      mart_crm_person.mql_date_first_pt,
      mart_crm_person.accepted_date,
      mart_crm_person.accepted_date_pt,
      mart_crm_person.qualifying_date,
      mart_crm_person.qualifying_date_pt,

  --Touchpoint Data
      'Person Touchpoint' AS touchpoint_type,
      mart_crm_touchpoint.bizible_touchpoint_date,
      mart_crm_touchpoint.bizible_touchpoint_position,
      mart_crm_touchpoint.bizible_touchpoint_source,
      mart_crm_touchpoint.bizible_touchpoint_type,
      mart_crm_touchpoint.bizible_ad_campaign_name,
      mart_crm_touchpoint.bizible_form_url,
      mart_crm_touchpoint.bizible_landing_page,
      mart_crm_touchpoint.bizible_form_url_raw,
      mart_crm_touchpoint.bizible_landing_page_raw,
      -- UTM Parameters 
      mart_crm_touchpoint.utm_campaign,
      mart_crm_touchpoint.utm_medium,
      mart_crm_touchpoint.utm_source,
      mart_crm_touchpoint.utm_budget,
      mart_crm_touchpoint.utm_content,
      mart_crm_touchpoint.utm_allptnr,
      mart_crm_touchpoint.utm_partnerid,
      mart_crm_touchpoint.integrated_budget_holder,
      mart_crm_touchpoint.touchpoint_offer_type,
      mart_crm_touchpoint.touchpoint_offer_type_grouped,
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
      mart_crm_touchpoint.keystone_content_name,
      mart_crm_touchpoint.keystone_gitlab_epic,
      mart_crm_touchpoint.keystone_gtm,
      mart_crm_touchpoint.keystone_url_slug,
      mart_crm_touchpoint.keystone_type,
      -- Touchpoint Data Cont.
      mart_crm_touchpoint.bizible_marketing_channel,
      mart_crm_touchpoint.bizible_marketing_channel_path,
      mart_crm_touchpoint.marketing_review_channel_grouping,
      mart_crm_touchpoint.bizible_medium,
      mart_crm_touchpoint.bizible_referrer_page,
      mart_crm_touchpoint.bizible_referrer_page_raw,
      mart_crm_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_touchpoint.bizible_salesforce_campaign,
      mart_crm_touchpoint.bizible_ad_group_name,
      mart_crm_touchpoint.campaign_rep_role_name,
      mart_crm_touchpoint.touchpoint_segment,
      mart_crm_touchpoint.gtm_motion,
      mart_crm_touchpoint.pipe_name,
      mart_crm_touchpoint.is_dg_influenced,
      mart_crm_touchpoint.is_dg_sourced,
      mart_crm_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_touchpoint.is_fmm_influenced,
      mart_crm_touchpoint.is_fmm_sourced,
      mart_crm_touchpoint.devrel_campaign_type,
      mart_crm_touchpoint.devrel_campaign_description,
      mart_crm_touchpoint.devrel_campaign_influence_type,
      mart_crm_touchpoint.bizible_count_lead_creation_touch AS new_lead_created_sum,
      mart_crm_touchpoint.count_true_inquiry AS count_true_inquiry,
      mart_crm_touchpoint.count_inquiry AS inquiry_sum, 
      mart_crm_touchpoint.pre_mql_weight AS mql_sum,
      mart_crm_touchpoint.count_accepted AS accepted_sum,
      mart_crm_touchpoint.count_net_new_mql AS new_mql_sum,
      mart_crm_touchpoint.count_net_new_accepted AS new_accepted_sum    
    FROM mart_crm_touchpoint
    LEFT JOIN mart_crm_person 
      ON mart_crm_touchpoint.dim_crm_person_id = mart_crm_person.dim_crm_person_id
       AND mart_crm_touchpoint.email_hash = mart_crm_person.email_hash
    LEFT JOIN map_alternative_lead_demographics
      ON mart_crm_touchpoint.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON mart_crm_person.dim_crm_account_id=dim_crm_account.dim_crm_account_id

), attribution_touchpoint_opportunity_calculations AS (

    SELECT
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_attribution_touchpoint.custom_net_arr,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_opportunity.is_sao,
      mart_crm_opportunity.is_won,
      CASE
          WHEN mart_crm_attribution_touchpoint.dim_crm_touchpoint_id IS NOT NULL THEN mart_crm_opportunity.dim_crm_opportunity_id
          ELSE null
      END AS influenced_opportunity_id,
      SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) AS custom_opp_created,
      CASE 
        WHEN mart_crm_opportunity.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS custom_sao,
      CASE 
        WHEN mart_crm_opportunity.is_sao = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS pipeline_custom_net_arr,
      CASE 
        WHEN mart_crm_opportunity.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.bizible_count_custom_model) 
        ELSE 0 
      END AS won_custom,
      CASE 
        WHEN mart_crm_opportunity.is_won = TRUE
          THEN SUM(mart_crm_attribution_touchpoint.custom_net_arr) 
        ELSE 0 
      END AS won_custom_net_arr
    FROM mart_crm_attribution_touchpoint
    LEFT JOIN mart_crm_opportunity
      ON mart_crm_attribution_touchpoint.dim_crm_opportunity_id=mart_crm_opportunity.dim_crm_opportunity_id
    {{dbt_utils.group_by(n=6)}}

  ), opp_base_with_batp AS (
    
    SELECT
    --IDs
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.dim_crm_account_id,
      dim_crm_account.dim_parent_crm_account_id,
      mart_crm_attribution_touchpoint.dim_crm_touchpoint_id,
      mart_crm_opportunity.dim_crm_user_id AS opp_dim_crm_user_id,
      mart_crm_opportunity.duplicate_opportunity_id,
      mart_crm_opportunity.merged_crm_opportunity_id,
      mart_crm_opportunity.ssp_id,
      mart_crm_opportunity.primary_campaign_source_id AS opp_primary_campaign_source_id,
      mart_crm_opportunity.owner_id AS opp_owner_id,
      mart_crm_attribution_touchpoint.dim_campaign_id,
      partner_account.crm_account_name AS partner_account_name,
      partner_account.dim_crm_account_id AS opp_partner_dim_crm_account_id,

	--Opp Dates
      mart_crm_opportunity.created_date AS opp_created_date,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.close_date,
      mart_crm_opportunity.subscription_start_date,
      mart_crm_opportunity.subscription_end_date,
      mart_crm_opportunity.pipeline_created_date,

	--Opp Flags
      mart_crm_opportunity.is_sao,
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.is_net_arr_closed_deal,
      mart_crm_opportunity.is_jihu_account,
      mart_crm_opportunity.is_closed,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.is_ps_opp,
      mart_crm_opportunity.is_win_rate_calc,
      mart_crm_opportunity.is_net_arr_pipeline_created,
      mart_crm_opportunity.is_new_logo_first_order,
      mart_crm_opportunity.is_closed_won,
      mart_crm_opportunity.is_web_portal_purchase,
      mart_crm_opportunity.is_lost,
      mart_crm_opportunity.is_open,
      mart_crm_opportunity.is_renewal,
      mart_crm_opportunity.is_duplicate,
      mart_crm_opportunity.is_refund,
      mart_crm_opportunity.is_deleted,
      mart_crm_opportunity.is_excluded_from_pipeline_created,
      mart_crm_opportunity.is_contract_reset,
      mart_crm_opportunity.is_booked_net_arr,
      mart_crm_opportunity.is_downgrade,
      mart_crm_opportunity.critical_deal_flag,
      mart_crm_opportunity.is_public_sector_opp,
      mart_crm_opportunity.is_registration_from_portal,

    --Opp Data
      mart_crm_opportunity.new_logo_count,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.amount,
      mart_crm_opportunity.invoice_number,
      mart_crm_opportunity.order_type AS opp_order_type,
      mart_crm_opportunity.sales_qualified_source_name,
      mart_crm_opportunity.deal_path_name,
      mart_crm_opportunity.subscription_type,
      mart_crm_opportunity.report_segment,
      mart_crm_opportunity.report_region,
      mart_crm_opportunity.report_area,
      mart_crm_opportunity.report_geo,
      mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
      mart_crm_opportunity.report_role_name,
      mart_crm_opportunity.report_role_level_1,
      mart_crm_opportunity.report_role_level_2,
      mart_crm_opportunity.report_role_level_3,
      mart_crm_opportunity.report_role_level_4,
      mart_crm_opportunity.report_role_level_5,
      mart_crm_opportunity.parent_crm_account_upa_country,
      mart_crm_opportunity.parent_crm_account_territory,
      mart_crm_opportunity.opportunity_name,
      mart_crm_opportunity.crm_account_name,
      mart_crm_opportunity.parent_crm_account_name,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.closed_buckets,
      mart_crm_opportunity.opportunity_category,
      mart_crm_opportunity.source_buckets AS opp_source_buckets,
      mart_crm_opportunity.crm_sales_dev_rep_id,
      mart_crm_opportunity.crm_business_dev_rep_id,
      mart_crm_opportunity.opportunity_development_representative,
      mart_crm_opportunity.sdr_or_bdr,
      mart_crm_opportunity.sdr_pipeline_contribution,
      mart_crm_opportunity.sales_path,
      mart_crm_opportunity.opportunity_deal_size,
      mart_crm_opportunity.net_new_source_categories AS opp_net_new_source_categories,
      mart_crm_opportunity.deal_path_engagement,
      mart_crm_opportunity.opportunity_owner,
      mart_crm_opportunity.order_type_grouped AS opp_order_type_grouped,
      mart_crm_opportunity.sales_qualified_source_grouped,
      mart_crm_opportunity.crm_account_gtm_strategy,
      mart_crm_opportunity.crm_account_focus_account,
      mart_crm_opportunity.lead_source AS opp_lead_source,
      mart_crm_opportunity.calculated_deal_count,
      mart_crm_opportunity.days_in_stage,
      mart_crm_opportunity.record_type_name,
      CASE
        WHEN mart_crm_opportunity.dr_deal_id IS NOT null
          THEN TRUE
        ELSE FALSE
      END AS is_created_through_deal_registration,
      mart_crm_opportunity.resale_partner_name,

    --Person Data
      mart_crm_person.dim_crm_person_id,
      mart_crm_person.dim_crm_user_id,
      mart_crm_person.crm_partner_id,
      mart_crm_person.sfdc_record_id,
      mart_crm_person.email_hash,
      mart_crm_person.email_domain,
      mart_crm_person.was_converted_lead,
      mart_crm_person.email_domain_type,
      mart_crm_person.is_valuable_signup,
      mart_crm_person.status AS crm_person_status,
      mart_crm_person.lead_source,
      mart_crm_person.source_buckets,
      mart_crm_person.is_mql,
      mart_crm_person.is_inquiry,
      mart_crm_person.is_lead_source_trial,
      mart_crm_person.account_demographics_sales_segment_grouped,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_segment_region_grouped,
      mart_crm_person.zoominfo_company_employee_count,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_territory,
      mart_crm_person.person_first_country,
      dim_crm_account.is_first_order_available,
      mart_crm_person.sales_segment_name AS person_sales_segment_name,
      mart_crm_person.sales_segment_grouped AS person_sales_segment_grouped,
      mart_crm_person.person_score,
      mart_crm_person.behavior_score,
      mart_crm_person.employee_bucket,
      mart_crm_person.leandata_matched_account_sales_segment,
      mart_crm_person.sfdc_record_type,
      map_alternative_lead_demographics.employee_count_segment_custom,
      map_alternative_lead_demographics.employee_bucket_segment_custom,
      COALESCE(map_alternative_lead_demographics.employee_count_segment_custom, 
      map_alternative_lead_demographics.employee_bucket_segment_custom) AS inferred_employee_segment,
      map_alternative_lead_demographics.geo_custom,
      UPPER(map_alternative_lead_demographics.geo_custom) AS inferred_geo,
      CASE
          WHEN mart_crm_person.is_first_order_person = TRUE 
            THEN '1. New - First Order'
          ELSE '3. Growth'
      END AS person_order_type,
      last_utm_campaign,
      last_utm_content,
      mart_crm_person.prospect_share_status,
      mart_crm_person.partner_prospect_status,
      mart_crm_person.lead_score_classification,
      mart_crm_person.is_defaulted_trial,
      mart_crm_person.is_exclude_from_reporting,

  --Person Dates
      mart_crm_person.true_inquiry_date_pt,
      mart_crm_person.mql_date_latest_pt,
      mart_crm_person.legacy_mql_date_first_pt,
      mart_crm_person.mql_sfdc_date_pt,
      mart_crm_person.mql_date_first_pt,
      mart_crm_person.accepted_date,
      mart_crm_person.accepted_date_pt,
      mart_crm_person.qualifying_date,
      mart_crm_person.qualifying_date_pt,
    
    -- Touchpoint Data
      'Attribution Touchpoint' AS touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_touchpoint_date,
      mart_crm_attribution_touchpoint.bizible_touchpoint_position,
      mart_crm_attribution_touchpoint.bizible_touchpoint_source,
      mart_crm_attribution_touchpoint.bizible_touchpoint_type,
      mart_crm_attribution_touchpoint.bizible_ad_campaign_name,
      mart_crm_attribution_touchpoint.bizible_ad_group_name,
      mart_crm_attribution_touchpoint.bizible_form_url,
      mart_crm_attribution_touchpoint.bizible_landing_page,
      mart_crm_attribution_touchpoint.bizible_form_url_raw,
      mart_crm_attribution_touchpoint.bizible_landing_page_raw,
      mart_crm_attribution_touchpoint.touchpoint_sales_stage AS opp_touchpoint_sales_stage,
      mart_crm_attribution_touchpoint.keystone_content_name,
      mart_crm_attribution_touchpoint.keystone_gitlab_epic,
      mart_crm_attribution_touchpoint.keystone_gtm,
      mart_crm_attribution_touchpoint.keystone_url_slug,
      mart_crm_attribution_touchpoint.keystone_type,
    -- UTM Parameters 
      mart_crm_attribution_touchpoint.utm_campaign,
      mart_crm_attribution_touchpoint.utm_medium,
      mart_crm_attribution_touchpoint.utm_source,
      mart_crm_attribution_touchpoint.utm_budget,
      mart_crm_attribution_touchpoint.utm_content,
      mart_crm_attribution_touchpoint.utm_allptnr,
      mart_crm_attribution_touchpoint.utm_partnerid,
      mart_crm_attribution_touchpoint.integrated_budget_holder,
      mart_crm_attribution_touchpoint.touchpoint_offer_type,
      mart_crm_attribution_touchpoint.touchpoint_offer_type_grouped,
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
      -- Touchpoint Data Cont.
      mart_crm_attribution_touchpoint.bizible_marketing_channel,
      mart_crm_attribution_touchpoint.bizible_marketing_channel_path,
      mart_crm_attribution_touchpoint.marketing_review_channel_grouping,
      mart_crm_attribution_touchpoint.bizible_medium,
      mart_crm_attribution_touchpoint.bizible_referrer_page,
      mart_crm_attribution_touchpoint.bizible_referrer_page_raw,
      mart_crm_attribution_touchpoint.bizible_integrated_campaign_grouping,
      mart_crm_attribution_touchpoint.bizible_salesforce_campaign,
	    mart_crm_attribution_touchpoint.campaign_rep_role_name,
      mart_crm_attribution_touchpoint.touchpoint_segment,
      mart_crm_attribution_touchpoint.gtm_motion,
      mart_crm_attribution_touchpoint.pipe_name,
      mart_crm_attribution_touchpoint.is_dg_influenced,
      mart_crm_attribution_touchpoint.is_dg_sourced,
      mart_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      mart_crm_attribution_touchpoint.bizible_count_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_custom_model,
      mart_crm_attribution_touchpoint.bizible_weight_first_touch,
      mart_crm_attribution_touchpoint.is_fmm_influenced,
      mart_crm_attribution_touchpoint.is_fmm_sourced,
      mart_crm_attribution_touchpoint.devrel_campaign_type,
      mart_crm_attribution_touchpoint.devrel_campaign_description,
      mart_crm_attribution_touchpoint.devrel_campaign_influence_type,
      attribution_touchpoint_opportunity_calculations.influenced_opportunity_id,
      attribution_touchpoint_opportunity_calculations.custom_sao,
      attribution_touchpoint_opportunity_calculations.pipeline_custom_net_arr,
      attribution_touchpoint_opportunity_calculations.won_custom,
      attribution_touchpoint_opportunity_calculations.won_custom_net_arr
    FROM mart_crm_attribution_touchpoint
    LEFT JOIN mart_crm_person
      ON mart_crm_attribution_touchpoint.dim_crm_person_id = mart_crm_person.dim_crm_person_id
    LEFT JOIN mart_crm_opportunity
      ON mart_crm_attribution_touchpoint.dim_crm_opportunity_id=mart_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN map_alternative_lead_demographics
      ON mart_crm_person.dim_crm_person_id=map_alternative_lead_demographics.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON mart_crm_opportunity.dim_crm_account_id=dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_account partner_account
      ON mart_crm_opportunity.partner_account=partner_account.dim_crm_account_id
    LEFT JOIN attribution_touchpoint_opportunity_calculations
      ON mart_crm_attribution_touchpoint.dim_crm_touchpoint_id=attribution_touchpoint_opportunity_calculations.dim_crm_touchpoint_id
    
), cohort_base_combined AS (
  
    SELECT
	--IDs
      dim_crm_person_id,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_crm_user_id,
      crm_partner_id,
      partner_prospect_id,
      sfdc_record_id,
      dim_crm_touchpoint_id,
      dim_campaign_id,
      NULL AS dim_crm_opportunity_id,
      NULL AS opp_dim_crm_user_id,
      NULL AS duplicate_opportunity_id,
      NULL AS merged_crm_opportunity_id,
      NULL AS ssp_id,
      NULL AS opp_primary_campaign_source_id,
      NULL AS opp_owner_id,
      NULL AS partner_account_name,

  --Person Data
      email_hash,
      email_domain,
      was_converted_lead,
      email_domain_type,
      is_valuable_signup,
      crm_person_status,
      lead_source,
      source_buckets,
      is_mql,
      is_inquiry,
      is_lead_source_trial,
      account_demographics_sales_segment_grouped,
      account_demographics_sales_segment,
      account_demographics_segment_region_grouped,
      zoominfo_company_employee_count,
      account_demographics_region,
      account_demographics_geo,
      account_demographics_area,
      account_demographics_upa_country,
      account_demographics_territory,
      person_first_country,
      is_first_order_available,
      person_order_type,
      person_sales_segment_name,
      person_sales_segment_grouped,
      person_score,
      behavior_score,
      employee_bucket,
      leandata_matched_account_sales_Segment,
      sfdc_record_type,
      employee_count_segment_custom,
      employee_bucket_segment_custom,
      inferred_employee_segment,
      geo_custom,
      inferred_geo,
      last_utm_campaign,
      last_utm_content,
      prospect_share_status,
      partner_prospect_status,
      lead_score_classification,
      is_defaulted_trial,
      is_exclude_from_reporting,

  --Person Dates
      true_inquiry_date_pt,
      mql_date_latest_pt,
      legacy_mql_date_first_pt,
      mql_sfdc_date_pt,
      mql_date_first_pt,
      accepted_date,
      accepted_date_pt,
      qualifying_date,
      qualifying_date_pt,
  
  --Opp Dates
      NULL AS opp_created_date,
      NULL AS sales_accepted_date,
      NULL AS close_date,
      NULL AS subscription_start_date,
      NULL AS subscription_end_date,
      NULL AS pipeline_created_date,

	--Opp Flags
      NULL AS is_sao,
      NULL AS is_won,
      NULL AS is_net_arr_closed_deal,
      NULL AS is_jihu_account,
      NULL AS is_closed,
      NULL AS is_edu_oss,
      NULL AS is_ps_opp,
      NULL AS is_win_rate_calc,
      NULL AS is_net_arr_pipeline_created,
      NULL AS is_new_logo_first_order,
      NULL AS is_closed_won,
      NULL AS is_web_portal_purchase,
      NULL AS is_lost,
      NULL AS is_open,
      NULL AS is_renewal,
      NULL AS is_duplicate,
      NULL AS is_refund,
      NULL AS is_deleted,
      NULL AS is_excluded_from_pipeline_created,
      NULL AS is_contract_reset,
      NULL AS is_booked_net_arr,
      NULL AS is_downgrade,
      NULL AS critical_deal_flag,
      NULL AS is_public_sector_opp,
      NULL AS is_registration_from_portal,
      NULL AS is_created_through_deal_registration,

    --Opp Data
      NULL AS new_logo_count,
      NULL AS net_arr,
      NULL AS amount,
      NULL AS invoice_number,
      NULL AS opp_order_type,
      NULL AS sales_qualified_source_name,
      NULL AS deal_path_name,
      NULL AS subscription_type,
      NULL AS report_segment,
      NULL AS report_region,
      NULL AS report_area,
      NULL AS report_geo,
      NULL AS parent_crm_account_geo_pubsec_segment,
      NULL AS report_role_name,
      NULL AS report_role_level_1,
      NULL AS report_role_level_2,
      NULL AS report_role_level_3,
      NULL AS report_role_level_4,
      NULL AS report_role_level_5,
      NULL AS parent_crm_account_upa_country,
      NULL AS parent_crm_account_territory,
      NULL AS opportunity_name,
      NULL AS crm_account_name,
      NULL AS parent_crm_account_name,
      NULL AS stage_name,
      NULL AS closed_buckets,
      NULL AS opportunity_category,
      NULL AS opp_source_buckets,
      NULL AS crm_sales_dev_rep_id,
      NULL AS crm_business_dev_rep_id,
      NULL AS opportunity_development_representative,
      NULL AS sdr_or_bdr,
      NULL AS sdr_pipeline_contribution,
      NULL AS sales_path,
      NULL AS opportunity_deal_size,
      NULL AS opp_net_new_source_categories,
      NULL AS deal_path_engagement,
      NULL AS opportunity_owner,
      NULL AS sales_qualified_source_grouped,
      NULL AS crm_account_gtm_strategy,
      NULL AS crm_account_focus_account,
      NULL AS opp_lead_source,
      NULL AS calculated_deal_count,
      NULL AS days_in_stage,
      NULL AS record_type_name,
      NULL as resale_partner_name,
  
  --Touchpoint Data
      touchpoint_type,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_landing_page,
      bizible_form_url_raw,
      bizible_landing_page_raw,
      NULL AS opp_touchpoint_sales_stage,
      keystone_content_name,
      keystone_gitlab_epic,
      keystone_gtm,
      keystone_url_slug,
      keystone_type,
      -- UTM Parameters 
      utm_campaign,
      utm_medium,
      utm_source,
      utm_budget,
      utm_content,
      utm_allptnr,
      utm_partnerid,
      integrated_budget_holder,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      utm_campaign_date,
      utm_campaign_region,
      utm_campaign_budget,
      utm_campaign_type,
      utm_campaign_gtm,
      utm_campaign_language,
      utm_campaign_name,
      utm_campaign_agency,
      utm_content_offer,
      utm_content_asset_type,
      utm_content_industry,
      -- Touchpoint Data Cont.
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      marketing_review_channel_grouping,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_integrated_campaign_grouping,
      bizible_salesforce_campaign,
	    campaign_rep_role_name,
      touchpoint_segment,
      gtm_motion,
      pipe_name,
      is_dg_influenced,
      is_dg_sourced,
      bizible_count_lead_creation_touch,
      is_fmm_influenced,
      is_fmm_sourced,
      devrel_campaign_type,
      devrel_campaign_description,
      devrel_campaign_influence_type,
      new_lead_created_sum,
      count_true_inquiry,
      inquiry_sum, 
      mql_sum,
      accepted_sum,
      new_mql_sum,
      new_accepted_sum,
      NULL AS bizible_count_custom_model,
      NULL AS bizible_weight_custom_model,
      NULL AS bizible_weight_first_touch,
      NULL AS influenced_opportunity_id,
      0 AS custom_sao,
      0 AS pipeline_custom_net_arr,
      0 AS won_custom,
      0 AS won_custom_net_arr
  FROM mart_crm_person_with_tp
  
  UNION ALL
  
    SELECT
    --IDs
      dim_crm_person_id,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_crm_user_id,
      crm_partner_id,
      NULL AS partner_prospect_id,
      sfdc_record_id,
      dim_crm_touchpoint_id,
      dim_campaign_id,
      dim_crm_opportunity_id,
      opp_dim_crm_user_id,
      duplicate_opportunity_id,
      merged_crm_opportunity_id,
      ssp_id,
      opp_primary_campaign_source_id,
      opp_owner_id,
      partner_account_name,

    --Person Data
      email_hash,
      email_domain,
      was_converted_lead,
      email_domain_type,
      is_valuable_signup,
      crm_person_status,
      lead_source,
      source_buckets,
      is_mql,
      is_inquiry,
      is_lead_source_trial,
      account_demographics_sales_segment_grouped,
      account_demographics_sales_segment,
      account_demographics_segment_region_grouped,
      zoominfo_company_employee_count,
      account_demographics_region,
      account_demographics_geo,
      account_demographics_area,
      account_demographics_upa_country,
      account_demographics_territory,
      person_first_country,
      is_first_order_available,
      person_order_type,
      person_sales_segment_name,
      person_sales_segment_grouped,
      person_score,
      behavior_score,
      employee_bucket,
      leandata_matched_account_sales_Segment,
      sfdc_record_type,
      employee_count_segment_custom,
      employee_bucket_segment_custom,
      inferred_employee_segment,
      geo_custom,
      inferred_geo,
      last_utm_campaign,
      last_utm_content,
      prospect_share_status,
      partner_prospect_status,
      lead_score_classification,
      is_defaulted_trial,
      is_exclude_from_reporting,
    
    --Person Dates
      true_inquiry_date_pt,
      mql_date_latest_pt,
      legacy_mql_date_first_pt,
      mql_sfdc_date_pt,
      mql_date_first_pt,
      accepted_date,
      accepted_date_pt,
      qualifying_date,
      qualifying_date_pt,

    --Opp Dates
      opp_created_date,
      sales_accepted_date,
      close_date,
      subscription_start_date,
      subscription_end_date,
      pipeline_created_date,

    --Opp Flags
      is_sao,
      is_won,
      is_net_arr_closed_deal,
      is_jihu_account,
      is_closed,
      is_edu_oss,
      is_ps_opp,
      is_win_rate_calc,
      is_net_arr_pipeline_created,
      is_new_logo_first_order,
      is_closed_won,
      is_web_portal_purchase,
      is_lost,
      is_open,
      is_renewal,
      is_duplicate,
      is_refund,
      is_deleted,
      is_excluded_from_pipeline_created,
      is_contract_reset,
      is_booked_net_arr,
      is_downgrade,
      critical_deal_flag,
      is_public_sector_opp,
      is_registration_from_portal,
      is_created_through_deal_registration,

      --Opp Data
      new_logo_count,
      net_arr,
      amount,
      invoice_number,
      opp_order_type,
      sales_qualified_source_name,
      deal_path_name,
      subscription_type,
      report_segment,
      report_region,
      report_area,
      report_geo,
      parent_crm_account_geo_pubsec_segment,
      report_role_name,
      report_role_level_1,
      report_role_level_2,
      report_role_level_3,
      report_role_level_4,
      report_role_level_5,
      parent_crm_account_upa_country,
      parent_crm_account_territory,
      opportunity_name,
      crm_account_name,
      parent_crm_account_name,
      stage_name,
      closed_buckets,
      opportunity_category,
      opp_source_buckets,
      crm_sales_dev_rep_id,
      crm_business_dev_rep_id,
      opportunity_development_representative,
      sdr_or_bdr,
      sdr_pipeline_contribution,
      sales_path,
      opportunity_deal_size,
      opp_net_new_source_categories,
      deal_path_engagement,
      opportunity_owner,
      sales_qualified_source_grouped,
      crm_account_gtm_strategy,
      crm_account_focus_account,
      opp_lead_source,
      calculated_deal_count,
      days_in_stage,
      record_type_name,
      resale_partner_name,
    
      --Touchpoint Data
      touchpoint_type,
      bizible_touchpoint_date,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_type,
      bizible_ad_campaign_name,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_landing_page,
      bizible_form_url_raw,
      bizible_landing_page_raw,
      opp_touchpoint_sales_stage,
      keystone_content_name,
      keystone_gitlab_epic,
      keystone_gtm,
      keystone_url_slug,
      keystone_type,
      -- UTM Parameters 
      utm_campaign,
      utm_medium,
      utm_source,
      utm_budget,
      utm_content,
      utm_allptnr,
      utm_partnerid,
      integrated_budget_holder,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      utm_campaign_date,
      utm_campaign_region,
      utm_campaign_budget,
      utm_campaign_type,
      utm_campaign_gtm,
      utm_campaign_language,
      utm_campaign_name,
      utm_campaign_agency,
      utm_content_offer,
      utm_content_asset_type,
      utm_content_industry,
      -- Touchpoint Data Cont.
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      marketing_review_channel_grouping,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_integrated_campaign_grouping,
      bizible_salesforce_campaign,
      campaign_rep_role_name,
      touchpoint_segment,
      gtm_motion,
      pipe_name,
      is_dg_influenced,
      is_dg_sourced,
      bizible_count_lead_creation_touch,
      is_fmm_influenced,
      is_fmm_sourced,
      devrel_campaign_type,
      devrel_campaign_description,
      devrel_campaign_influence_type,
      0 AS new_lead_created_sum,
      0 AS count_true_inquiry,
      0 AS inquiry_sum, 
      0 AS mql_sum,
      0 AS accepted_sum,
      0 AS new_mql_sum,
      0 AS new_accepted_sum,
      bizible_count_custom_model,
      bizible_weight_custom_model,
      bizible_weight_first_touch,
      influenced_opportunity_id,
      custom_sao,
      pipeline_custom_net_arr,
      won_custom,
      won_custom_net_arr
    FROM opp_base_with_batp

), person_history_base AS (
    SELECT
      sfdc_lead_history.lead_id AS sfdc_record_id,
      cohort_base_combined.dim_crm_touchpoint_id,
      sfdc_lead_history.field_modified_at,
      cohort_base_combined.bizible_touchpoint_date,
      datediff(DAY, 
        cohort_base_combined.bizible_touchpoint_date,
        sfdc_lead_history.field_modified_at
      ) AS date_difference,
      sfdc_lead_history.old_value_string,
      sfdc_lead_history.new_value_string,
      'Lead' AS record_type
    FROM sfdc_lead_history
      INNER JOIN cohort_base_combined
        ON sfdc_lead_history.lead_id = cohort_base_combined.sfdc_record_id 
    WHERE 
    lead_field = 'status' AND field_modified_at >= bizible_touchpoint_date
    
    UNION ALL 

    SELECT
      sfdc_contact_history.contact_id AS sfdc_record_id,
      cohort_base_combined.dim_crm_touchpoint_id,
      sfdc_contact_history.field_modified_at,
      cohort_base_combined.bizible_touchpoint_date,
      datediff(DAY, 
        cohort_base_combined.bizible_touchpoint_date,
        sfdc_contact_history.field_modified_at
      ) AS date_difference,
      sfdc_contact_history.old_value_string,
      sfdc_contact_history.new_value_string,
      'Contact' as record_type
    FROM sfdc_contact_history
      INNER JOIN cohort_base_combined
        ON sfdc_contact_history.contact_id = cohort_base_combined.sfdc_record_id 
    WHERE contact_field = 'contact_status__c' AND field_modified_at >= bizible_touchpoint_date

), person_history_final as (
  SELECT
  person_history_base.*,
  person_history_base.old_value_string || ' -> ' || person_history_base.new_value_string as person_status_change,
  CASE WHEN person_status_change IS NULL THEN
    '[No Update]' 
    ELSE person_status_change
  END AS person_status_update,
  row_number() OVER (PARTITION BY sfdc_record_id, dim_crm_touchpoint_id ORDER BY field_modified_at ASC) AS history_update_rank
  FROM
  person_history_base
  QUALIFY history_update_rank = 1

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM dim_date
  WHERE date_actual = CURRENT_DATE

), intermediate AS (
  
    SELECT 
      cohort_base_combined.*,
      PARSE_URL(bizible_form_url_raw):parameters:utm_asset_type::VARCHAR    AS bizible_form_page_utm_asset_type,
      PARSE_URL(bizible_landing_page_raw):parameters:utm_asset_type::VARCHAR AS bizible_landing_page_utm_asset_type,
      COALESCE(bizible_landing_page_utm_asset_type, bizible_form_page_utm_asset_type) AS utm_asset_type,
      
      -- campaign
      fct_campaign.start_date  AS campaign_start_date,
      fct_campaign.region      AS sfdc_campaign_region,
      fct_campaign.sub_region  AS sfdc_campaign_sub_region,
      dim_campaign.type        AS sfdc_campaign_type,
      fct_campaign.budgeted_cost,
      fct_campaign.actual_cost,
      dim_campaign.is_a_channel_partner_involved,
      dim_campaign.is_an_alliance_partner_involved,
      dim_campaign.channel_partner_name,
      dim_campaign.alliance_partner_name,
      campaign_owner.user_name          AS campaign_owner,
      campaign_owner_manager.user_name  AS campaign_owner_manager,
      fct_campaign.dim_parent_campaign_id,
      parent_campaign.campaign_name     AS parent_campaign_name,
      dim_campaign.series_campaign_id,
      dim_campaign.series_campaign_name,
      CASE  
        WHEN dim_campaign.will_there_be_mdf_funding = 'Yes'
          THEN TRUE
          ELSE FALSE
      END AS is_mdf_campaign,

      -- Account
      dim_crm_account.abm_tier,
      dim_crm_account.health_number,
      dim_crm_account.crm_account_type,

     -- Opportunity Report Fields
     CASE 
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.crm_user_business_unit
        ELSE opportunity_owner.crm_user_business_unit
    END                                                       AS report_opportunity_user_business_unit,
    CASE 
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.crm_user_sub_business_unit
        ELSE opportunity_owner.crm_user_sub_business_unit
    END                                                       AS report_opportunity_user_sub_business_unit,
    CASE 
        WHEN account_owner.is_hybrid_user = 1 
            THEN dim_crm_account.parent_crm_account_area
        WHEN cohort_base_combined.close_date < today.current_fiscal_year_date
          THEN account_owner.asm
        WHEN UPPER(opportunity_owner.crm_user_area) = 'ALL'
           THEN dim_crm_account.parent_crm_account_area         
        ELSE opportunity_owner.asm
    END                                                       AS report_opportunity_user_asm,

      -- user
      user.user_name        AS record_owner_name,
      user.manager_name     AS record_owner_manager,
      user.title            AS record_owner_title,
      user.department       AS record_owner_department,
      user.team             AS record_owner_team,
      manager.manager_name  AS record_owner_sales_dev_leader,
      
      CASE
        WHEN  record_owner_title LIKE '%Sales Development%' 
          OR record_owner_title  LIKE '%Business Development%' 
        THEN TRUE
        ELSE FALSE
      END AS is_sales_dev_owned_record,

      person_history_final.person_status_update,
      person_history_final.person_status_change,

     --inquiry_date fields
      inquiry_date.fiscal_year                     AS inquiry_date_range_year,
      inquiry_date.fiscal_quarter_name_fy          AS inquiry_date_range_quarter,
      DATE_TRUNC(month, inquiry_date.date_actual)  AS inquiry_date_range_month,
      inquiry_date.first_day_of_week               AS inquiry_date_range_week,
      inquiry_date.date_id                         AS inquiry_date_range_id,
    
    --mql_date fields
      mql_date.fiscal_year                         AS mql_date_range_year,
      mql_date.fiscal_quarter_name_fy              AS mql_date_range_quarter,
      DATE_TRUNC(month, mql_date.date_actual)      AS mql_date_range_month,
      mql_date.first_day_of_week                   AS mql_date_range_week,
      mql_date.date_id                             AS mql_date_range_id,
  
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

    --pipeline_created_date fields
      pipeline_created_date.fiscal_year                     AS pipeline_created_date_range_year,
      pipeline_created_date.fiscal_quarter_name_fy          AS pipeline_created_date_range_quarter,
      DATE_TRUNC(month, pipeline_created_date.date_actual)  AS pipeline_created_date_range_month,
      pipeline_created_date.first_day_of_week               AS pipeline_created_date_range_week,
      pipeline_created_date.date_id                         AS pipeline_created_date_range_id,
  
    --closed_date fields
      closed_date.fiscal_year                     AS closed_date_range_year,
      closed_date.fiscal_quarter_name_fy          AS closed_date_range_quarter,
      DATE_TRUNC(month, closed_date.date_actual)  AS closed_date_range_month,
      closed_date.first_day_of_week               AS closed_date_range_week,
      closed_date.date_id                         AS closed_date_range_id,

    --bizible_date fields
      bizible_date.fiscal_year                     AS bizible_date_range_year,
      bizible_date.fiscal_quarter_name_fy          AS bizible_date_range_quarter,
      DATE_TRUNC(month, bizible_date.date_actual)  AS bizible_date_range_month,
      bizible_date.first_day_of_week               AS bizible_date_range_week,
      bizible_date.date_id                         AS bizible_date_range_id
  FROM cohort_base_combined
  CROSS JOIN today
  LEFT JOIN dim_campaign
    ON cohort_base_combined.dim_campaign_id = dim_campaign.dim_campaign_id
  LEFT JOIN fct_campaign
    ON cohort_base_combined.dim_campaign_id = fct_campaign.dim_campaign_id
  LEFT JOIN dim_campaign parent_campaign
    ON fct_campaign.dim_parent_campaign_id = parent_campaign.dim_campaign_id
  LEFT JOIN dim_crm_user user
    ON cohort_base_combined.dim_crm_user_id = user.dim_crm_user_id
  LEFT JOIN dim_crm_user manager
    ON user.manager_id = manager.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner
    ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user campaign_owner_manager
    ON campaign_owner.manager_id = campaign_owner_manager.dim_crm_user_id
  LEFT JOIN dim_crm_account
    ON cohort_base_combined.dim_crm_account_id=dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user account_owner
    ON dim_crm_account.dim_crm_user_id = account_owner.dim_crm_user_id
  LEFT JOIN dim_crm_user opportunity_owner
    ON cohort_base_combined.opp_dim_crm_user_id = opportunity_owner.dim_crm_user_id
  LEFT JOIN person_history_final
    ON cohort_base_combined.dim_crm_touchpoint_id = person_history_final.dim_crm_touchpoint_id 
    AND cohort_base_combined.sfdc_record_id = person_history_final.sfdc_record_id
  LEFT JOIN dim_date inquiry_date
    ON cohort_base_combined.true_inquiry_date_pt = inquiry_date.date_day
  LEFT JOIN dim_date mql_date
    ON cohort_base_combined.mql_date_latest_pt = mql_date.date_day
  LEFT JOIN dim_date opp_create_date
    ON cohort_base_combined.opp_created_date = opp_create_date.date_day
  LEFT JOIN dim_date sao_date
    ON cohort_base_combined.sales_accepted_date = sao_date.date_day
  LEFT JOIN dim_date closed_date
    ON cohort_base_combined.close_date=closed_date.date_day
  LEFT JOIN dim_date pipeline_created_date
    ON cohort_base_combined.pipeline_created_date=pipeline_created_date.date_day
  LEFT JOIN dim_date bizible_date
    ON cohort_base_combined.bizible_touchpoint_date=bizible_date.date_day

)

SELECT *
FROM intermediate
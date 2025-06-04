{{ config(materialized='table') }}

{{ simple_cte([
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('dim_crm_user','dim_crm_user'),
    ('mart_crm_person','mart_crm_person'),
    ('sfdc_lead','sfdc_lead'),
    ('mart_crm_account','mart_crm_account'),
    ('mart_crm_event','mart_crm_event'),
    ('mart_crm_task','mart_crm_task'),
    ('mart_team_member_directory','mart_team_member_directory'),
    ('bdg_crm_opportunity_contact_role','bdg_crm_opportunity_contact_role'),
    ('dim_date', 'dim_date'),
    ('dim_sales_dev_user_hierarchy', 'dim_sales_dev_user_hierarchy'),
    ('wk_marketo_activity_change_score', 'wk_marketo_activity_change_score')
  ]) 
}}

, sales_dev_opps AS (

  SELECT 
    mart_crm_opportunity.dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    mart_crm_opportunity.dim_crm_opportunity_id,
    mart_crm_opportunity.net_arr,
    mart_crm_opportunity.net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_1,
    mart_crm_opportunity.xdr_net_arr_stage_3,
    mart_crm_opportunity.sales_accepted_date AS sales_accepted_date,
    mart_crm_opportunity.sales_accepted_fiscal_quarter_name,
    dim_date.day_of_fiscal_quarter AS sao_day_of_fiscal_quarter,
    mart_crm_opportunity.stage_0_pending_acceptance_date,
    mart_crm_opportunity.stage_0_pending_acceptance_month,
    mart_crm_opportunity.stage_0_pending_acceptance_fiscal_quarter_name,
    mart_crm_opportunity.stage_1_discovery_date,
    mart_crm_opportunity.stage_1_discovery_month,
    mart_crm_opportunity.stage_1_discovery_fiscal_quarter_name,
    mart_crm_opportunity.stage_2_scoping_date,
    mart_crm_opportunity.stage_2_scoping_month,
    mart_crm_opportunity.stage_2_scoping_fiscal_quarter_name,
    mart_crm_opportunity.stage_3_technical_evaluation_date,
    mart_crm_opportunity.stage_3_technical_evaluation_month,
    mart_crm_opportunity.stage_3_technical_evaluation_fiscal_quarter_name,
    mart_crm_opportunity.pipeline_created_date,
    mart_crm_opportunity.pipeline_created_month,
    mart_crm_opportunity.pipeline_created_fiscal_quarter_name, 
    mart_crm_opportunity.pipeline_created_fiscal_year,
    mart_crm_opportunity.days_in_1_discovery,
    mart_crm_opportunity.days_in_sao,
    mart_crm_opportunity.days_since_last_activity,
    mart_crm_opportunity.sales_qualified_source_name,
    mart_crm_opportunity.report_segment,
    mart_crm_opportunity.report_geo,
    mart_crm_opportunity.report_region,
    mart_crm_opportunity.report_area,
    mart_crm_opportunity.parent_crm_account_geo_pubsec_segment,
    mart_crm_opportunity.report_role_level_1,
    mart_crm_opportunity.report_role_level_2,
    SPLIT_PART(mart_crm_opportunity.report_role_level_2, '_', 2)                         AS report_role_level_2_clean,
    mart_crm_opportunity.report_role_level_3,
    COALESCE(SPLIT_PART(mart_crm_opportunity.report_role_level_3, '_', 3),SPLIT_PART(mart_crm_opportunity.report_role_level_2, '_', 2) )                         AS report_role_level_3_clean,
    mart_crm_opportunity.report_role_level_4,
    mart_crm_opportunity.report_role_level_5,
    mart_crm_opportunity.pipe_council_grouping,
    mart_crm_opportunity.parent_crm_account_territory,
    mart_crm_opportunity.parent_crm_account_sales_segment,
    mart_crm_opportunity.parent_crm_account_geo,
    mart_crm_opportunity.parent_crm_account_region,
    mart_crm_opportunity.parent_crm_account_area,
    mart_crm_opportunity.deal_path_name,
    mart_crm_opportunity.created_date AS opp_created_date,
    mart_crm_opportunity.close_date,
    mart_crm_opportunity.order_type,
    mart_crm_opportunity.opportunity_category,
    mart_crm_opportunity.stage_name,
    mart_crm_opportunity.product_category,
    mart_crm_opportunity.product_details,
    mart_crm_opportunity.products_purchased,
    mart_crm_opportunity.crm_account_focus_account, /* 
    mart_crm_opportunity.crm_opp_owner_sales_segment_stamped,
    mart_crm_opportunity.crm_opp_owner_business_unit_stamped,
    mart_crm_opportunity.crm_opp_owner_geo_stamped,
    mart_crm_opportunity.crm_opp_owner_region_stamped,
    mart_crm_opportunity.crm_opp_owner_area_stamped,*/
    mart_crm_opportunity.is_sao,
    mart_crm_opportunity.is_net_arr_closed_deal,
    mart_crm_opportunity.is_net_arr_pipeline_created,
    mart_crm_opportunity.is_eligible_age_analysis,
    mart_crm_opportunity.is_eligible_open_pipeline,
    mart_crm_opportunity.crm_business_dev_rep_id AS opportunity_business_development_representative,
    mart_crm_opportunity.crm_sales_dev_rep_id AS opportunity_sales_development_representative,
    mart_crm_account.bdr_next_steps,
    mart_crm_account.bdr_account_research,
    mart_crm_account.bdr_account_strategy,
    mart_crm_account.account_bdr_assigned_user_role,
    mart_crm_account.bdr_recycle_date,
    mart_crm_account.actively_working_start_date,
    CASE 
    WHEN mart_crm_opportunity.crm_business_dev_rep_id IS NOT NULL 
    THEN 'BDR' 
    WHEN mart_crm_opportunity.crm_sales_dev_rep_id IS NOT NULL 
    THEN 'SDR' 
    END AS sales_dev_bdr_or_sdr,
    COALESCE(mart_crm_opportunity.crm_business_dev_rep_id,mart_crm_opportunity.crm_sales_dev_rep_id) AS sdr_bdr_user_id
    FROM mart_crm_opportunity
    LEFT JOIN dim_date 
      ON mart_crm_opportunity.sales_accepted_date = dim_date.date_day
    LEFT JOIN mart_crm_account 
      ON mart_crm_opportunity.dim_crm_account_id = mart_crm_account.dim_crm_account_id
    WHERE sdr_bdr_user_id IS NOT NULL 
      AND stage_1_discovery_date >= '2023-02-01' 

), merged_person_base AS (

  SELECT DISTINCT  
    mart_crm_person.dim_crm_person_id,
    sfdc_lead.converted_contact_id AS sfdc_record_id,
    sfdc_lead.lead_id AS original_lead_id,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date,
    mart_crm_person.dim_crm_account_id
  FROM sfdc_lead 
  LEFT JOIN mart_crm_person 
    ON mart_crm_person.sfdc_record_id = sfdc_lead.converted_contact_id  
  LEFT JOIN sales_dev_opps 
    ON converted_opportunity_id = dim_crm_opportunity_id 
  WHERE converted_contact_id IS NOT NULL 

), contacts_on_opps AS (

  SELECT 
    bdg_crm_opportunity_contact_role.sfdc_record_id AS sfdc_record_id,
    bdg_crm_opportunity_contact_role.dim_crm_person_id AS dim_crm_person_id,
    bdg_crm_opportunity_contact_role.contact_role,
    bdg_crm_opportunity_contact_role.is_primary_contact,
    sales_dev_opps.dim_crm_opportunity_id,
    sales_dev_opps.opp_created_date AS opp_created_date,
    sales_dev_opps.sales_accepted_date AS sales_accepted_date,
    sales_dev_opps.sdr_bdr_user_id AS dim_crm_user_id 
  FROM bdg_crm_opportunity_contact_role
  INNER JOIN sales_dev_opps
    ON sales_dev_opps.dim_crm_opportunity_id = bdg_crm_opportunity_contact_role.dim_crm_opportunity_id
  
), activity_base AS (

  SELECT 
    mart_crm_event.event_id AS activity_id,
    mart_crm_event.dim_crm_user_id,
    mart_crm_event.dim_crm_opportunity_id,
    mart_crm_event.dim_crm_account_id,
    mart_crm_event.sfdc_record_id,
    mart_crm_event.dim_crm_person_id,
    dim_crm_user.dim_crm_user_id AS booked_by_user_id,
    mart_crm_event.event_date AS activity_date,
    dim_date.day_of_fiscal_quarter as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy as activity_fiscal_quarter_name,
    'Event' AS activity_type,
    mart_crm_event.event_type AS activity_subtype
  FROM mart_crm_event
  LEFT JOIN dim_crm_user 
    ON booked_by_employee_number = dim_crm_user.employee_number 
    LEFT JOIN dim_date 
    ON mart_crm_event.event_date = dim_date.date_day
  INNER JOIN sales_dev_opps 
    ON mart_crm_event.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
      OR booked_by_user_id = sales_dev_opps.sdr_bdr_user_id 
  WHERE activity_date >= '2023-01-01'
  UNION
  SELECT 
    mart_crm_task.task_id AS activity_id,
    mart_crm_task.dim_crm_user_id,
    mart_crm_task.dim_crm_opportunity_id,
    mart_crm_task.dim_crm_account_id,
    mart_crm_task.sfdc_record_id,
    mart_crm_task.dim_crm_person_id,
    NULL AS booked_by_user_id,
    DATE(mart_crm_task.task_completed_date) AS activity_date,
    dim_date.day_of_fiscal_quarter as activity_day_of_fiscal_quarter,
    dim_date.fiscal_quarter_name_fy as activity_fiscal_quarter_name,
    mart_crm_task.task_type AS activity_type,
    mart_crm_task.task_subtype AS activity_subtype
  FROM mart_crm_task
  INNER JOIN sales_dev_opps 
    ON mart_crm_task.dim_crm_user_id = sales_dev_opps.sdr_bdr_user_id
  LEFT JOIN dim_date 
    ON DATE(mart_crm_task.task_completed_date) = dim_date.date_day
  WHERE activity_date >= '2023-01-01'

), activity_final AS (

  SELECT DISTINCT 
    activity_base.activity_id,
    COALESCE(activity_base.booked_by_user_id,activity_base.dim_crm_user_id) AS dim_crm_user_id,
    mart_crm_person.dim_crm_person_id,
    COALESCE(mart_crm_person.sfdc_record_id, activity_base.sfdc_record_id) AS sfdc_record_id,
    COALESCE(mart_crm_person.dim_crm_account_id, activity_base.dim_crm_account_id) AS dim_crm_account_id,
    activity_base.activity_date::DATE AS activity_date,
    activity_base.activity_day_of_fiscal_quarter,
    activity_base.activity_fiscal_quarter_name,
    activity_base.activity_type,
    activity_base.activity_subtype
  FROM activity_base
  LEFT JOIN merged_person_base 
    ON activity_base.sfdc_record_id = merged_person_base.original_lead_id
  LEFT JOIN mart_crm_person 
    ON COALESCE(merged_person_base.dim_crm_person_id,activity_base.dim_crm_person_id) = mart_crm_person.dim_crm_person_id    
  
), opp_to_lead AS (

  SELECT DISTINCT
    sales_dev_opps.*,
    merged_person_base.dim_crm_person_id AS converted_person_id,
    contacts_on_opps.dim_crm_person_id AS contact_person_id,
    activity_final.dim_crm_person_id AS activity_person_id,
    COALESCE(merged_person_base.dim_crm_person_id,contacts_on_opps.dim_crm_person_id,activity_final.dim_crm_person_id) AS waterfall_person_id,
    COALESCE(DATEDIFF(DAY,activity_date,sales_dev_opps.sales_accepted_date),0) AS activity_to_sao_days
  FROM sales_dev_opps
  LEFT JOIN merged_person_base 
    ON sales_dev_opps.dim_crm_opportunity_id = merged_person_base.dim_crm_opportunity_id
  LEFT JOIN contacts_on_opps 
    ON sales_dev_opps.dim_crm_opportunity_id = contacts_on_opps.dim_crm_opportunity_id
  LEFT JOIN activity_final
    ON sales_dev_opps.dim_crm_account_id = activity_final.dim_crm_account_id 
      AND activity_final.activity_date <= sales_dev_opps.sales_accepted_date 
      AND sales_dev_opps.sdr_bdr_user_id = activity_final.dim_crm_user_id

), opps_missing_link AS (

  SELECT * 
  FROM opp_to_lead 
  WHERE waterfall_person_id IS NULL OR activity_to_sao_days > 90 --adds back in the opps that are being discarded due to a too long delay from activity on the lead for that lead to be credited with SAO creation

), sixsense_6qa_lead_flag_base AS (

  SELECT 
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    wk_marketo_activity_change_score.new_value - wk_marketo_activity_change_score.old_value AS score_change,
    min(activity_date) AS first_6qa_score_date,
    max(activity_date) AS last_6qa_score_date
  FROM 
    wk_marketo_activity_change_score 
  LEFT JOIN mart_crm_person 
    ON wk_marketo_activity_change_score.lead_id = mart_crm_person.marketo_lead_id 
  WHERE reason = 'Changed by Smart Campaign OP-Scoring_2020.6QA Identified action Change Score' AND primary_attribute_value = 'Behavior Score'
  GROUP BY ALL



), final AS (

  SELECT 
    mart_crm_person.dim_crm_person_id,
    mart_crm_person.sfdc_record_id,
    COALESCE(opp_to_lead.dim_crm_account_id,mart_crm_person.dim_crm_account_id) AS dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    mart_crm_person.mql_date_latest,
    dim_mql_date.day_of_fiscal_quarter AS mql_day_of_fiscal_quarter,
    dim_mql_date.fiscal_quarter_name_fy AS mql_fiscal_quarter_name,
    mql_date_first.fiscal_quarter_name_fy AS first_mql_fiscal_quarter_name,
    mql_date_first.day_of_fiscal_quarter AS first_mql_day_of_fiscal_quarter,
    mql_date_first.date_day AS first_mql_date,
    mart_crm_person.inquiry_date_pt,
    mart_crm_person.true_inquiry_date,
    mart_crm_person.high_priority_datetime,
    dim_inquiry_date.day_of_fiscal_quarter AS inquiry_day_of_fiscal_quarter,
    dim_inquiry_date.fiscal_quarter_name_fy AS inquiry_fiscal_quarter_name,
    mart_crm_person.account_demographics_sales_segment AS person_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped AS person_sales_segment_grouped,
    mart_crm_person.account_demographics_geo AS person_first_geo,
    mart_crm_person.is_mql,
    mart_crm_person.is_first_order_person,
    mart_crm_person.person_first_country,
    mart_crm_person.lead_score_classification,
    mart_crm_person.is_defaulted_trial,
    mart_crm_person.lead_source,
    mart_crm_person.status AS lead_status,
    mart_crm_person.bizible_mql_form_url,
    mart_crm_person.bizible_mql_ad_campaign_name,
    mart_crm_person.bizible_mql_marketing_channel,
    mart_crm_person.bizible_mql_marketing_channel_path,
    mart_crm_person.bizible_most_recent_form_url,
    mart_crm_person.bizible_most_recent_ad_campaign_name,
    mart_crm_person.bizible_most_recent_marketing_channel,
    mart_crm_person.bizible_most_recent_marketing_channel_path,
    mart_crm_person.source_buckets,
    mart_crm_person.email_domain_type,
    mart_crm_person.sfdc_record_type,
    mart_crm_person.mql_worked_by_user_id,
    mart_crm_person.mql_worked_by_user_manager_id,
    mart_crm_person.last_worked_by_date,
    mart_crm_person.last_worked_by_datetime,
    mart_crm_person.last_worked_by_user_manager_id,
    mart_crm_person.last_worked_by_user_id,
    CASE 
      WHEN mart_crm_person.propensity_to_purchase_score_group IS NULL 
        THEN 'No PTP Score' 
      ELSE mart_crm_person.propensity_to_purchase_score_group 
    END AS propensity_to_purchase_score_group,
    CASE 
      WHEN propensity_to_purchase_score_group = '4' OR propensity_to_purchase_score_group = '5' 
        THEN TRUE 
      ELSE FALSE 
    END AS is_high_ptp_lead,
    mart_crm_person.marketo_last_interesting_moment,
    mart_crm_person.marketo_last_interesting_moment_date,
    CASE 
      WHEN sixsense_6qa_lead_flag_base.dim_crm_person_id IS NOT NULL 
        THEN TRUE 
      ELSE FALSE 
    END AS is_6qa_scored_lead,
    sixsense_6qa_lead_flag_base.first_6qa_score_date,
    sixsense_6qa_lead_flag_base.last_6qa_score_date,
    activity_final.dim_crm_user_id,
    activity_final.activity_date,
    activity_final.activity_type,
    activity_final.activity_subtype,
    activity_final.activity_id,
    activity_final.activity_day_of_fiscal_quarter,
    activity_final.activity_fiscal_quarter_name,
    NULL as tasks_completed,
    CASE 
      WHEN activity_final.activity_date >= mart_crm_person.mql_date_latest 
        THEN TRUE 
      ELSE FALSE 
    END AS worked_after_mql_flag,

    mart_crm_account.parent_crm_account_territory,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.abm_tier,
    mart_crm_account.crm_account_owner_id,
    mart_crm_account.crm_account_owner,
    mart_crm_account.owner_role,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.crm_account_owner_user_segment,
    mart_crm_account.six_sense_account_profile_fit,
    mart_crm_account.six_sense_account_reach_score,
    mart_crm_account.six_sense_account_profile_score,
    mart_crm_account.six_sense_account_buying_stage,
    mart_crm_account.six_sense_account_numerical_reach_score,
    mart_crm_account.six_sense_account_update_date,
    mart_crm_account.six_sense_account_6_qa_start_date,
    mart_crm_account.six_sense_account_6_qa_end_date,
    mart_crm_account.six_sense_account_6_qa_age_days,
    mart_crm_account.six_sense_account_intent_score,
    mart_crm_account.six_sense_segments,
    mart_crm_account.pte_score_group,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.is_first_order_available,
    mart_crm_account.crm_account_type,
    mart_crm_account.crm_account_industry,
    mart_crm_account.crm_account_sub_industry,
    mart_crm_account.bdr_next_steps,
    mart_crm_account.bdr_account_research,
    mart_crm_account.bdr_account_strategy,
    mart_crm_account.account_bdr_assigned_user_role,
    mart_crm_account.bdr_recycle_date,
    mart_crm_account.actively_working_start_date,
    opp_to_lead.dim_crm_opportunity_id,
    opp_to_lead.sdr_bdr_user_id,
    opp_to_lead.net_arr,
    opp_to_lead.net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_1,
    opp_to_lead.xdr_net_arr_stage_3,
    opp_to_lead.sales_accepted_date,
    opp_to_lead.sales_accepted_fiscal_quarter_name,
    opp_to_lead.sao_day_of_fiscal_quarter,
    opp_to_lead.stage_0_pending_acceptance_date,
    opp_to_lead.stage_0_pending_acceptance_month,
    opp_to_lead.stage_0_pending_acceptance_fiscal_quarter_name,
    opp_to_lead.stage_1_discovery_date,
    opp_to_lead.stage_1_discovery_month,
    opp_to_lead.stage_1_discovery_fiscal_quarter_name,
    opp_to_lead.stage_2_scoping_date,
    opp_to_lead.stage_2_scoping_month,
    opp_to_lead.stage_2_scoping_fiscal_quarter_name,
    opp_to_lead.stage_3_technical_evaluation_date,
    opp_to_lead.stage_3_technical_evaluation_month,
    opp_to_lead.stage_3_technical_evaluation_fiscal_quarter_name, 
    opp_to_lead.pipeline_created_date,
    opp_to_lead.pipeline_created_month,
    opp_to_lead.pipeline_created_fiscal_quarter_name, 
    opp_to_lead.pipeline_created_fiscal_year,
    opp_to_lead.days_in_1_discovery,
    opp_to_lead.days_in_sao,
    opp_to_lead.days_since_last_activity,
    opp_to_lead.sales_qualified_source_name,
    opp_to_lead.report_segment,
    opp_to_lead.report_geo,
    opp_to_lead.report_region,
    opp_to_lead.report_area,
    opp_to_lead.parent_crm_account_geo_pubsec_segment,
    opp_to_lead.report_role_level_1,
    opp_to_lead.report_role_level_2,
    opp_to_lead.report_role_level_2_clean,
    opp_to_lead.report_role_level_3,
    opp_to_lead.report_role_level_3_clean,
    opp_to_lead.report_role_level_4,
    opp_to_lead.report_role_level_5,
    opp_to_lead.pipe_council_grouping,
    opp_to_lead.deal_path_name,
    opp_to_lead.opp_created_date,
    opp_to_lead.close_date,
    opp_to_lead.activity_to_sao_days,
    opp_to_lead.order_type,
    opp_to_lead.opportunity_category,
    opp_to_lead.stage_name,
    opp_to_lead.product_category,
    opp_to_lead.product_details,
    opp_to_lead.products_purchased,
    opp_to_lead.sales_dev_bdr_or_sdr,
    opp_to_lead.opportunity_sales_development_representative,
    opp_to_lead.opportunity_business_development_representative,
    /* 
    opp_to_lead.crm_opp_owner_sales_segment_stamped,
    opp_to_lead.crm_opp_owner_business_unit_stamped,
    opp_to_lead.crm_opp_owner_geo_stamped,
    opp_to_lead.crm_opp_owner_region_stamped,
    opp_to_lead.crm_opp_owner_area_stamped,*/
    opp_to_lead.is_sao,
    opp_to_lead.is_net_arr_closed_deal,
    opp_to_lead.is_net_arr_pipeline_created,
    opp_to_lead.is_eligible_age_analysis,
    opp_to_lead.is_eligible_open_pipeline,
    opportunity_snapshot_hierarchy.dim_crm_user_id              AS sales_dev_rep_user_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_email,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_full_name AS sales_dev_rep_full_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_title,
    opportunity_snapshot_hierarchy.sales_dev_rep_department,
    opportunity_snapshot_hierarchy.sales_dev_rep_team,
    opportunity_snapshot_hierarchy.sales_dev_rep_is_active,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_1,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_2,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_3,
    opportunity_snapshot_hierarchy.crm_user_sales_segment,
    opportunity_snapshot_hierarchy.crm_user_geo,
    opportunity_snapshot_hierarchy.crm_user_region,
    opportunity_snapshot_hierarchy.crm_user_area,
    opportunity_snapshot_hierarchy.sales_dev_rep_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_rep_direct_manager_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_manager_full_name as sales_dev_manager_full_name,
    opportunity_snapshot_hierarchy.sales_dev_manager_email,
    opportunity_snapshot_hierarchy.sales_dev_manager_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_manager_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_id,
    opportunity_snapshot_hierarchy.sales_dev_leader_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_leader_full_name as sales_dev_leader_full_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_leader_email,
    activity_snapshot_hierarchy.dim_crm_user_id                 AS activity_sales_dev_rep_user_id,
    activity_snapshot_hierarchy.sales_dev_rep_role_name         AS activity_sales_dev_rep_role_name,
    activity_snapshot_hierarchy.sales_dev_rep_email             AS activity_sales_dev_rep_email,
    activity_snapshot_hierarchy.sales_dev_rep_user_full_name    AS activity_sales_dev_rep_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_manager_full_name AS activity_sales_dev_manager_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_leader_full_name  AS activity_sales_dev_leader_full_name,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_1 AS activity_sales_dev_rep_user_role_level_1,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_2 AS activity_sales_dev_rep_user_role_level_2,
    activity_snapshot_hierarchy.sales_dev_rep_user_role_level_3 AS activity_sales_dev_rep_user_role_level_3

  FROM mart_crm_person
  LEFT JOIN dim_date dim_mql_date
   ON mart_crm_person.mql_date_latest_pt = dim_mql_date.date_day 
  LEFT JOIN dim_date mql_date_first
   ON mart_crm_person.mql_date_first_pt = mql_date_first.date_day 
  LEFT JOIN dim_date dim_inquiry_date
   ON mart_crm_person.true_inquiry_date = dim_inquiry_date.date_day 
  LEFT JOIN sixsense_6qa_lead_flag_base 
   ON mart_crm_person.dim_crm_person_id = sixsense_6qa_lead_flag_base.dim_crm_person_id
  LEFT JOIN activity_final
    ON mart_crm_person.dim_crm_person_id = activity_final.dim_crm_person_id 
  LEFT JOIN opp_to_lead 
    ON mart_crm_person.dim_crm_person_id = opp_to_lead.waterfall_person_id
  LEFT JOIN mart_crm_account 
    ON COALESCE(opp_to_lead.dim_crm_account_id,mart_crm_person.dim_crm_account_id) = mart_crm_account.dim_crm_account_id
  LEFT JOIN dim_sales_dev_user_hierarchy opportunity_snapshot_hierarchy
    ON opp_to_lead.sdr_bdr_user_id = opportunity_snapshot_hierarchy.dim_crm_user_id 
    AND opp_to_lead.stage_1_discovery_date = opportunity_snapshot_hierarchy.snapshot_date

  LEFT JOIN dim_sales_dev_user_hierarchy activity_snapshot_hierarchy
    ON activity_final.dim_crm_user_id = activity_snapshot_hierarchy.dim_crm_user_id 
    AND activity_final.activity_date = activity_snapshot_hierarchy.snapshot_date
  WHERE activity_to_sao_days <= 90 OR activity_to_sao_days IS NULL 
  UNION 
  SELECT DISTINCT -- distinct is necessary in order to not duplicate rows as addition of the rule above of activity_to_sao_days >90 might create multiple rows if there are multiple leads that satisfy the condition per opp which is not ideal. 
    NULL AS dim_crm_person_id,
    NULL AS sfdc_record_id,
    opps_missing_link.dim_crm_account_id AS dim_crm_account_id,
    mart_crm_account.bdr_prospecting_status,
    NULL AS mql_date_latest,
    NULL AS mql_day_of_fiscal_quarter,
    NULL AS mql_fiscal_quarter_name,
    NULL AS first_mql_fiscal_quarter_name,
    NULL AS first_mql_day_of_fiscal_quarter,
    NULL AS first_mql_date,
    NULL AS inquiry_date_pt,
    NULL AS true_inquiry_date,
    NULL AS high_priority_datetime,
    NULL AS inquiry_day_of_fiscal_quarter,
    NULL AS inquiry_fiscal_quarter_name,
    NULL AS person_sales_segment,
    NULL AS person_sales_segment_grouped,
    NULL AS person_first_geo,
    NULL AS is_mql,
    NULL AS is_first_order_person,
    NULL AS person_first_country,
    NULL AS lead_score_classification,
    NULL AS is_defaulted_trial,
    NULL AS lead_source,
    NULL AS lead_status,
    NULL AS bizible_mql_form_url,
    NULL AS bizible_mql_ad_campaign_name,
    NULL AS bizible_mql_marketing_channel,
    NULL AS bizible_mql_marketing_channel_path,
    NULL AS bizible_most_recent_form_url,
    NULL AS bizible_most_recent_ad_campaign_name,
    NULL AS bizible_most_recent_marketing_channel,
    NULL AS bizible_most_recent_marketing_channel_path,
    NULL AS source_buckets,
    NULL AS email_domain_type,
    NULL AS sfdc_record_type,
    NULL AS mql_worked_by_user_id,
    NULL AS mql_worked_by_user_manager_id,
    NULL AS last_worked_by_date,
    NULL AS last_worked_by_datetime,
    NULL AS last_worked_by_user_manager_id,
    NULL AS last_worked_by_user_id,
    NULL AS propensity_to_purchase_score_group,
    NULL AS is_high_ptp_lead,
    NULL AS marketo_last_interesting_moment,
    NULL AS marketo_last_interesting_moment_date,
    NULL AS is_6qa_scored_lead,
    NULL AS first_6qa_score_date,
    NULL AS last_6qa_score_date,
    NULL AS dim_crm_user_id,
    NULL AS activity_date,
    NULL AS activity_type,
    NULL AS activity_subtype,
    NULL AS activity_id,
    NULL AS activity_day_of_fiscal_quarter,
    NULL AS activity_fiscal_quarter_name,
    NULL AS tasks_completed,
    NULL AS worked_after_mql_flag,
    mart_crm_account.parent_crm_account_territory,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.abm_tier,
    mart_crm_account.crm_account_owner_id,
    mart_crm_account.crm_account_owner,
    mart_crm_account.owner_role,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.crm_account_owner_user_segment,
    mart_crm_account.six_sense_account_profile_fit,
    mart_crm_account.six_sense_account_reach_score,
    mart_crm_account.six_sense_account_profile_score,
    mart_crm_account.six_sense_account_buying_stage,
    mart_crm_account.six_sense_account_numerical_reach_score,
    mart_crm_account.six_sense_account_update_date,
    mart_crm_account.six_sense_account_6_qa_start_date,
    mart_crm_account.six_sense_account_6_qa_end_date,
    mart_crm_account.six_sense_account_6_qa_age_days,
    mart_crm_account.six_sense_account_intent_score,
    mart_crm_account.six_sense_segments,
    mart_crm_account.pte_score_group,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.is_first_order_available,
    mart_crm_account.crm_account_type,
    mart_crm_account.crm_account_industry,
    mart_crm_account.crm_account_sub_industry,
    mart_crm_account.bdr_next_steps,
    mart_crm_account.bdr_account_research,
    mart_crm_account.bdr_account_strategy,
    mart_crm_account.account_bdr_assigned_user_role,
    mart_crm_account.bdr_recycle_date,
    mart_crm_account.actively_working_start_date,
    opps_missing_link.dim_crm_opportunity_id,
    opps_missing_link.sdr_bdr_user_id,
    opps_missing_link.net_arr,
    opps_missing_link.net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_1,
    opps_missing_link.xdr_net_arr_stage_3,
    opps_missing_link.sales_accepted_date,
    opps_missing_link.sales_accepted_fiscal_quarter_name,
    opps_missing_link.sao_day_of_fiscal_quarter,
    opps_missing_link.stage_0_pending_acceptance_date,
    opps_missing_link.stage_0_pending_acceptance_month,
    opps_missing_link.stage_0_pending_acceptance_fiscal_quarter_name,
    opps_missing_link.stage_1_discovery_date,
    opps_missing_link.stage_1_discovery_month,
    opps_missing_link.stage_1_discovery_fiscal_quarter_name,
    opps_missing_link.stage_2_scoping_date,
    opps_missing_link.stage_2_scoping_month,
    opps_missing_link.stage_2_scoping_fiscal_quarter_name,
    opps_missing_link.stage_3_technical_evaluation_date,
    opps_missing_link.stage_3_technical_evaluation_month,
    opps_missing_link.stage_3_technical_evaluation_fiscal_quarter_name, 
    opps_missing_link.pipeline_created_date,
    opps_missing_link.pipeline_created_month,
    opps_missing_link.pipeline_created_fiscal_quarter_name, 
    opps_missing_link.pipeline_created_fiscal_year,
    opps_missing_link.days_in_1_discovery,
    opps_missing_link.days_in_sao,
    opps_missing_link.days_since_last_activity,
    opps_missing_link.sales_qualified_source_name,
    opps_missing_link.report_segment,
    opps_missing_link.report_geo,
    opps_missing_link.report_region,
    opps_missing_link.report_area,
    opps_missing_link.parent_crm_account_geo_pubsec_segment,
    opps_missing_link.report_role_level_1,
    opps_missing_link.report_role_level_2,
    opps_missing_link.report_role_level_2_clean,
    opps_missing_link.report_role_level_3,
    opps_missing_link.report_role_level_3_clean,
    opps_missing_link.report_role_level_4,
    opps_missing_link.report_role_level_5,
    opps_missing_link.pipe_council_grouping,
    opps_missing_link.deal_path_name,
    opps_missing_link.opp_created_date,
    opps_missing_link.close_date,
    opps_missing_link.activity_to_sao_days,
    opps_missing_link.order_type,
    opps_missing_link.opportunity_category,
    opps_missing_link.stage_name,
    opps_missing_link.product_category,
    opps_missing_link.product_details,
    opps_missing_link.products_purchased,
    opps_missing_link.sales_dev_bdr_or_sdr,
    opps_missing_link.opportunity_sales_development_representative,
    opps_missing_link.opportunity_business_development_representative,
    /* 
    opps_missing_link.crm_opp_owner_sales_segment_stamped,
    opps_missing_link.crm_opp_owner_business_unit_stamped,
    opps_missing_link.crm_opp_owner_geo_stamped,
    opps_missing_link.crm_opp_owner_region_stamped,
    opps_missing_link.crm_opp_owner_area_stamped,*/
    opps_missing_link.is_sao,
    opps_missing_link.is_net_arr_closed_deal,
    opps_missing_link.is_net_arr_pipeline_created,
    opps_missing_link.is_eligible_age_analysis,
    opps_missing_link.is_eligible_open_pipeline,
    opportunity_snapshot_hierarchy.dim_crm_user_id AS sales_dev_rep_user_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_email,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_full_name as sales_dev_rep_full_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_title,
    opportunity_snapshot_hierarchy.sales_dev_rep_department,
    opportunity_snapshot_hierarchy.sales_dev_rep_team,
    opportunity_snapshot_hierarchy.sales_dev_rep_is_active,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_1,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_2,
    opportunity_snapshot_hierarchy.sales_dev_rep_user_role_level_3,
    opportunity_snapshot_hierarchy.crm_user_sales_segment,
    opportunity_snapshot_hierarchy.crm_user_geo,
    opportunity_snapshot_hierarchy.crm_user_region,
    opportunity_snapshot_hierarchy.crm_user_area,
    opportunity_snapshot_hierarchy.sales_dev_rep_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_rep_direct_manager_id,
    opportunity_snapshot_hierarchy.sales_dev_rep_manager_full_name AS sales_dev_manager_full_name,
    opportunity_snapshot_hierarchy.sales_dev_manager_email,
    opportunity_snapshot_hierarchy.sales_dev_manager_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_manager_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_id,
    opportunity_snapshot_hierarchy.sales_dev_leader_user_role_name,
    opportunity_snapshot_hierarchy.sales_dev_rep_leader_full_name AS sales_dev_leader_full_name,
    opportunity_snapshot_hierarchy.sales_dev_leader_employee_number,
    opportunity_snapshot_hierarchy.sales_dev_leader_email,
    NULL AS activity_sales_dev_rep_user_id,
    NULL AS activity_sales_dev_rep_role_name,
    NULL AS activity_sales_dev_rep_email,
    NULL AS activity_sales_dev_rep_full_name,
    NULL AS activity_sales_Dev_manager_full_name,
    NULL AS activity_sales_dev_leader_full_name,
    NULL AS activity_sales_dev_rep_user_role_level_1,
    NULL AS activity_sales_dev_rep_user_role_level_2,
    NULL AS activity_sales_dev_rep_user_role_level_3
  FROM opps_missing_link
  LEFT JOIN dim_sales_dev_user_hierarchy opportunity_snapshot_hierarchy
    ON opps_missing_link.sdr_bdr_user_id = opportunity_snapshot_hierarchy.dim_crm_user_id 
    AND opps_missing_link.stage_1_discovery_date = opportunity_snapshot_hierarchy.snapshot_date
  LEFT JOIN mart_crm_account 
    ON opps_missing_link.dim_crm_account_id = mart_crm_account.dim_crm_account_id

)

SELECT *
FROM final
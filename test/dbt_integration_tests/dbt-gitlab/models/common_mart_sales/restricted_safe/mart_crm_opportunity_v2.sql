{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user_hierarchy', 'dim_crm_user_hierarchy'),
    ('dim_crm_opportunity_flags', 'dim_crm_opportunity_flags'),
    ('dim_crm_opportunity_partner', 'dim_crm_opportunity_partner'),
    ('dim_crm_command_plan', 'dim_crm_command_plan'),
    ('dim_crm_opportunity_source_and_path', 'dim_crm_opportunity_source_and_path'),
    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_crm_opportunity_deal', 'dim_crm_opportunity_deal'),
    ('dim_order_type','dim_order_type'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_channel_type','dim_channel_type'),
    ('dim_sales_dev_user_hierarchy','dim_sales_dev_user_hierarchy')
]) }},

dim_crm_opportunity AS (

  SELECT *
  FROM {{ ref('dim_crm_opportunity', v=2) }}

),

fct_crm_opportunity AS (

  SELECT *
  FROM {{ ref('fct_crm_opportunity', v=2) }}

),

dim_crm_user AS (

  SELECT 
    dim_crm_user_id,
    user_name
  FROM {{ ref('dim_crm_user') }}

),

dates_fields_for_opportunities AS (

  {{ get_opportunity_date_fields() }}

),

final as (

  SELECT
    -- Primary Key
    fct_crm_opportunity.dim_crm_opportunity_id,

    -- Other keys

    fct_crm_opportunity.merged_crm_opportunity_id,
    fct_crm_opportunity.dim_parent_crm_opportunity_id,
    fct_crm_opportunity.duplicate_opportunity_id,
    fct_crm_opportunity.dim_parent_crm_account_id,
    dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk,
    fct_crm_opportunity.dim_crm_user_id,
    fct_crm_opportunity.dim_crm_user_id                           AS owner_id,
    dim_crm_user.user_name                                        AS opportunity_owner,
    fct_crm_opportunity.crm_sales_dev_rep_id,
    fct_crm_opportunity.crm_business_dev_rep_id,
    fct_crm_opportunity.record_type_id,
    fct_crm_opportunity.ssp_id,
    fct_crm_opportunity.ga_client_id,
    fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    fct_crm_opportunity.contract_reset_opportunity_id,
    fct_crm_opportunity.invoice_number,

    -- Opportunity details
    dim_crm_opportunity.opportunity_name,
    dim_crm_opportunity.forecast_category_name,
    dim_crm_opportunity.opportunity_category,
    dim_crm_opportunity.tam_notes,
    dim_crm_opportunity.opportunity_term,
    dim_crm_opportunity.growth_type,
    dim_crm_opportunity.deployment_preference,
    dim_crm_opportunity.record_type_name,
    dim_crm_opportunity.qsr_notes,
    dim_crm_opportunity.qsr_status,
    dim_crm_opportunity.startup_type,
    dim_crm_opportunity.next_steps,
    dim_crm_opportunity.competitors,
    dim_crm_opportunity.solutions_to_be_replaced,
    dim_crm_opportunity.professional_services_value,
    dim_crm_opportunity.edu_services_value,
    dim_crm_opportunity.investment_services_value,
    dim_crm_opportunity.primary_solution_architect,
    dim_crm_opportunity.product_category,
    dim_crm_opportunity.product_details,
    dim_crm_opportunity.products_purchased,
    dim_crm_opportunity.intended_product_tier,
    dim_crm_opportunity.opportunity_health,
    dim_crm_opportunity.risk_type,
    dim_crm_opportunity.risk_reasons,
    dim_crm_opportunity.manager_confidence,
    dim_crm_opportunity.reason_for_loss,
    dim_crm_opportunity.reason_for_loss_details,
    dim_crm_opportunity.reason_for_loss_staged,
    dim_crm_opportunity.reason_for_loss_calc,
    dim_crm_opportunity.downgrade_reason,
    dim_crm_opportunity.downgrade_details,
    dim_crm_opportunity.churn_contraction_type,
    dim_crm_opportunity.churn_contraction_net_arr_bucket,
    dim_crm_opportunity.auto_renewal_status,
    dim_crm_opportunity.renewal_risk_category,
    dim_crm_opportunity.renewal_swing_arr,
    dim_crm_opportunity.renewal_manager,
    dim_crm_opportunity.renewal_forecast_health,
  
    -- Useful flags
    dim_crm_opportunity_flags.is_won,
    dim_crm_opportunity_flags.is_closed,
    dim_crm_opportunity_flags.is_lost,
    dim_crm_opportunity_flags.is_open,
    dim_crm_opportunity_flags.is_active,
    dim_crm_opportunity_flags.is_stage_1_plus,
    dim_crm_opportunity_flags.is_stage_3_plus,
    dim_crm_opportunity_flags.is_stage_4_plus,
    dim_crm_opportunity_flags.is_closed_won,
    dim_crm_opportunity_flags.is_edu_oss,
    dim_crm_opportunity_flags.is_ps_opp,
    dim_crm_opportunity_flags.is_public_sector_opp,
    dim_crm_opportunity_flags.is_sao,
    dim_crm_opportunity_flags.is_new_logo_first_order,
    dim_crm_opportunity_flags.is_renewal,
    dim_crm_opportunity_flags.is_refund,
    dim_crm_opportunity_flags.is_web_portal_purchase,
    dim_crm_opportunity_flags.is_registration_from_portal,
    dim_crm_opportunity_flags.is_credit,
    dim_crm_opportunity_flags.is_downgrade,
    dim_crm_opportunity_flags.is_contract_reset,
    dim_crm_opportunity_flags.fpa_master_bookings_flag,
    dim_crm_opportunity_flags.is_net_arr_pipeline_created,
    dim_crm_opportunity_flags.is_net_arr_closed_deal,
    dim_crm_opportunity_flags.is_booked_net_arr,
    dim_crm_opportunity_flags.is_win_rate_calc,
    dim_crm_opportunity_flags.is_risky,
    dim_crm_opportunity_flags.critical_deal_flag,
    dim_crm_opportunity_flags.valid_deal_count,
    dim_crm_opportunity_flags.is_abm_tier_sao,
    dim_crm_opportunity_flags.is_abm_tier_closed_won,
    dim_crm_opportunity_flags.is_deleted,
    dim_crm_opportunity_flags.is_duplicate,
    dim_crm_opportunity_flags.is_excluded_from_pipeline_created,
    dim_crm_opportunity_flags.is_comp_new_logo_override,
    dim_crm_opportunity_flags.is_eligible_open_pipeline,
    dim_crm_opportunity_flags.is_eligible_asp_analysis,
    dim_crm_opportunity_flags.is_eligible_age_analysis,
    dim_crm_opportunity_flags.is_eligible_churn_contraction,
    dim_crm_opportunity_flags.competitors_other_flag,
    dim_crm_opportunity_flags.competitors_gitlab_core_flag,
    dim_crm_opportunity_flags.competitors_none_flag,
    dim_crm_opportunity_flags.competitors_github_enterprise_flag,
    dim_crm_opportunity_flags.competitors_bitbucket_server_flag,
    dim_crm_opportunity_flags.competitors_unknown_flag,
    dim_crm_opportunity_flags.competitors_github_flag,
    dim_crm_opportunity_flags.competitors_gitlab_flag,
    dim_crm_opportunity_flags.competitors_jenkins_flag,
    dim_crm_opportunity_flags.competitors_azure_devops_flag,
    dim_crm_opportunity_flags.competitors_svn_flag,
    dim_crm_opportunity_flags.competitors_bitbucket_flag,
    dim_crm_opportunity_flags.competitors_atlassian_flag,
    dim_crm_opportunity_flags.competitors_perforce_flag,
    dim_crm_opportunity_flags.competitors_visual_studio_flag,
    dim_crm_opportunity_flags.competitors_azure_flag,
    dim_crm_opportunity_flags.competitors_amazon_code_commit_flag,
    dim_crm_opportunity_flags.competitors_circleci_flag,
    dim_crm_opportunity_flags.competitors_bamboo_flag,
    dim_crm_opportunity_flags.competitors_aws_flag,
   
    -- Account fields
    dim_crm_account.dim_crm_account_id,
    dim_crm_account.crm_account_name,
    dim_crm_account.parent_crm_account_name,
    dim_crm_account.parent_crm_account_sales_segment,
    dim_crm_account.parent_crm_account_geo,
    dim_crm_account.parent_crm_account_geo_pubsec_segment,
    dim_crm_account.parent_crm_account_territory,
    dim_crm_account.parent_crm_account_region,
    dim_crm_account.parent_crm_account_area,
    dim_crm_account.parent_crm_account_business_unit,
    dim_crm_account.parent_crm_account_role_type,
    dim_crm_account.parent_crm_account_max_family_employee,
    dim_crm_account.parent_crm_account_upa_country,
    dim_crm_account.parent_crm_account_upa_state,
    dim_crm_account.parent_crm_account_upa_city,
    dim_crm_account.parent_crm_account_upa_street,
    dim_crm_account.parent_crm_account_upa_postal_code,
    dim_crm_account.owner_role                                    AS account_user_role,
    dim_crm_account.crm_account_employee_count,
    dim_crm_account.crm_account_gtm_strategy,
    dim_crm_account.crm_account_focus_account,
    dim_crm_account.crm_account_zi_technologies,
    dim_crm_account.is_jihu_account,
    dim_crm_account.admin_manual_source_number_of_employees,
    dim_crm_account.admin_manual_source_account_address,
    dim_crm_account.parent_crm_account_lam_dev_count,
    dim_crm_account.compensation_target_account                   AS is_compensation_target_account,
    dim_crm_account.is_base_prospect_account,
    CASE
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment != 'SMB' 
        THEN dim_crm_user_hierarchy.crm_user_sales_segment
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment = 'SMB'
        AND dim_crm_account.is_base_prospect_account = TRUE
        AND dim_crm_account.crm_account_employee_count >= 4000 
        THEN 'Base - Large'
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment = 'SMB'
        AND dim_crm_account.is_base_prospect_account = TRUE
        AND dim_crm_account.crm_account_employee_count < 4000 
        THEN 'Base - Mid-Market'
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment = 'SMB' 
        AND dim_crm_account.is_base_prospect_account = FALSE 
      THEN 'SMB - minus base' END                                 AS base_prospect_report_segment,

    CASE 
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment != 'SMB' 
        THEN TRUE 
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment = 'SMB' 
        AND dim_crm_account.is_base_prospect_account = TRUE
        THEN TRUE 
      WHEN dim_crm_user_hierarchy.crm_user_sales_segment = 'SMB' 
        AND dim_crm_account.is_base_prospect_account = FALSE 
        THEN FALSE
      ELSE FALSE END                                              AS is_mid_market_plus,

    -- User Hierarchy fields
    dim_crm_user_hierarchy.crm_user_sales_segment                 AS report_segment,
    dim_crm_user_hierarchy.crm_user_geo                           AS report_geo,
    dim_crm_user_hierarchy.crm_user_geo_pubsec_segment            AS report_geo_pubsec_segment,
    dim_crm_user_hierarchy.crm_user_region                        AS report_region,
    dim_crm_user_hierarchy.crm_user_area                          AS report_area,
    dim_crm_user_hierarchy.crm_user_business_unit                 AS report_business_unit,
    dim_crm_user_hierarchy.crm_user_role_name                     AS report_role_name,
    dim_crm_user_hierarchy.crm_user_role_level_1                  AS report_role_level_1,
    dim_crm_user_hierarchy.crm_user_role_level_2                  AS report_role_level_2,
    dim_crm_user_hierarchy.crm_user_role_level_3                  AS report_role_level_3,
    dim_crm_user_hierarchy.crm_user_role_level_4                  AS report_role_level_4,
    dim_crm_user_hierarchy.crm_user_role_level_5                  AS report_role_level_5,
    dim_crm_user_hierarchy.pipe_council_grouping,

    -- Opportunity Owner
    dim_crm_opportunity.opportunity_owner_role,
    dim_crm_opportunity.opportunity_owner_manager,
    dim_crm_opportunity.opportunity_owner_department,
    dim_crm_opportunity.opportunity_owner_title,


    -- Sales Qualified Source fields
    dim_sales_qualified_source.sales_qualified_source_name,
    dim_sales_qualified_source.sales_qualified_source_grouped,
    dim_sales_qualified_source.sqs_bucket_engagement,
    dim_order_type.order_type_name                                AS order_type,
    dim_order_type.order_type_grouped,
    dim_order_type_current.order_type_name                        AS order_type_current,

    --sales dev hierarchy fields
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_manager_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_leader_full_name,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_1,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_2,
    dim_sales_dev_user_hierarchy.sales_dev_rep_user_role_level_3,

    -- Partner Fields
    dim_crm_opportunity_partner.dr_partner_engagement             AS dr_partner_engagement_name,
    dim_crm_opportunity_partner.dr_partner_deal_type,
    dim_crm_opportunity_partner.dr_partner_engagement,
    dim_crm_opportunity_partner.dr_status,
    dim_crm_opportunity_partner.dr_primary_registration,
    dim_crm_opportunity_partner.dr_deal_id,
    dim_crm_opportunity_partner.aggregate_partner,
    dim_crm_opportunity_partner.calculated_partner_track,
    dim_crm_opportunity_partner.partner_account,
    dim_crm_opportunity_partner.partner_discount,
    dim_crm_opportunity_partner.partner_discount_calc,
    dim_crm_opportunity_partner.partner_margin_percentage,
    dim_crm_opportunity_partner.partner_track,
    dim_crm_opportunity_partner.platform_partner,
    dim_crm_opportunity_partner.resale_partner_track,
    dim_crm_opportunity_partner.resale_partner_name,
    dim_crm_opportunity_partner.fulfillment_partner,
    dim_crm_opportunity_partner.influence_partner,
    dim_crm_opportunity_partner.comp_channel_neutral,
    dim_crm_opportunity_partner.distributor,
    dim_crm_opportunity_partner.fulfillment_partner_account_name  AS fulfillment_partner_name,
    partner_account.crm_account_name                              AS partner_account_name,
    partner_account.gitlab_partner_program                        AS partner_gitlab_program,
    dim_alliance_type.alliance_type_name,
    dim_alliance_type.alliance_type_short_name,

    -- Command Plan Fields
    dim_crm_command_plan.cp_why_do_anything_at_all,
    dim_crm_command_plan.cp_why_now,
    dim_crm_command_plan.cp_identify_pain,
    dim_crm_command_plan.cp_metrics,
    dim_crm_command_plan.cp_value_driver,
    dim_crm_command_plan.cp_why_gitlab,
    dim_crm_command_plan.cp_champion,
    dim_crm_command_plan.cp_economic_buyer,
    dim_crm_command_plan.cp_partner,
    dim_crm_command_plan.cp_decision_process,
    dim_crm_command_plan.cp_decision_criteria,
    dim_crm_command_plan.cp_paper_process,
    dim_crm_command_plan.cp_use_cases,
    dim_crm_command_plan.cp_help,
    dim_crm_command_plan.cp_close_plan,
    dim_crm_command_plan.cp_review_notes,
    dim_crm_command_plan.cp_risks,
    dim_crm_command_plan.cp_score,

    -- Source and Path of the Opportunity
    dim_crm_opportunity_source_and_path.primary_campaign_source_id,
    dim_crm_opportunity_source_and_path.generated_source,
    dim_crm_opportunity_source_and_path.lead_source,
    dim_crm_opportunity_source_and_path.net_new_source_categories,
    dim_crm_opportunity_source_and_path.sales_path,
    dim_crm_opportunity_source_and_path.subscription_type,
    dim_crm_opportunity_source_and_path.source_buckets,
    dim_crm_opportunity_source_and_path.opportunity_development_representative,
    dim_crm_opportunity_source_and_path.sdr_or_bdr,
    dim_crm_opportunity_source_and_path.iqm_submitted_by_role,
    dim_crm_opportunity_source_and_path.sdr_pipeline_contribution,
    dim_channel_type.channel_type_name,

    -- Deal related fields
    dim_crm_opportunity_deal.deal_path                            AS deal_path_name,
    dim_crm_opportunity_deal.opportunity_deal_size,
    dim_crm_opportunity_deal.deal_category,
    dim_crm_opportunity_deal.deal_group,
    dim_crm_opportunity_deal.deal_size,
    dim_crm_opportunity_deal.calculated_deal_size,
    dim_crm_opportunity_deal.deal_path_engagement,
  
    -- VSA fields
    dim_crm_opportunity.vsa_readout,
    dim_crm_opportunity.vsa_start_date,
    dim_crm_opportunity.vsa_url,
    dim_crm_opportunity.vsa_status,
    dim_crm_opportunity.vsa_end_date,

    -- Dates
    dates_fields_for_opportunities.created_date,
    dates_fields_for_opportunities.created_month,
    dates_fields_for_opportunities.created_fiscal_quarter_date,
    dates_fields_for_opportunities.created_fiscal_quarter_name,
    dates_fields_for_opportunities.created_fiscal_year,
    dates_fields_for_opportunities.sales_accepted_date,
    dates_fields_for_opportunities.sales_accepted_month,
    dates_fields_for_opportunities.sales_accepted_fiscal_quarter_date,
    dates_fields_for_opportunities.sales_accepted_fiscal_quarter_name,
    dates_fields_for_opportunities.sales_accepted_fiscal_year,
    dates_fields_for_opportunities.close_date,
    dates_fields_for_opportunities.close_month,
    dates_fields_for_opportunities.close_fiscal_quarter_date,
    dates_fields_for_opportunities.close_fiscal_quarter_name,
    dates_fields_for_opportunities.close_fiscal_year,
    dates_fields_for_opportunities.stage_0_pending_acceptance_date,
    dates_fields_for_opportunities.stage_0_pending_acceptance_month,
    dates_fields_for_opportunities.stage_0_pending_acceptance_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_0_pending_acceptance_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_0_pending_acceptance_fiscal_year,
    dates_fields_for_opportunities.stage_1_discovery_date,
    dates_fields_for_opportunities.stage_1_discovery_month,
    dates_fields_for_opportunities.stage_1_discovery_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_1_discovery_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_1_discovery_fiscal_year,
    dates_fields_for_opportunities.stage_2_scoping_date,
    dates_fields_for_opportunities.stage_2_scoping_month,
    dates_fields_for_opportunities.stage_2_scoping_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_2_scoping_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_2_scoping_fiscal_year,
    dates_fields_for_opportunities.stage_3_technical_evaluation_date,
    dates_fields_for_opportunities.stage_3_technical_evaluation_month,
    dates_fields_for_opportunities.stage_3_technical_evaluation_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_3_technical_evaluation_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_3_technical_evaluation_fiscal_year,
    dates_fields_for_opportunities.stage_4_proposal_date,
    dates_fields_for_opportunities.stage_4_proposal_month,
    dates_fields_for_opportunities.stage_4_proposal_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_4_proposal_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_4_proposal_fiscal_year,
    dates_fields_for_opportunities.stage_5_negotiating_date,
    dates_fields_for_opportunities.stage_5_negotiating_month,
    dates_fields_for_opportunities.stage_5_negotiating_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_5_negotiating_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_5_negotiating_fiscal_year,
    dates_fields_for_opportunities.stage_6_awaiting_signature_date_date,
    dates_fields_for_opportunities.stage_6_awaiting_signature_date_month,
    dates_fields_for_opportunities.stage_6_awaiting_signature_date_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_6_awaiting_signature_date_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_6_awaiting_signature_date_fiscal_year,
    dates_fields_for_opportunities.stage_6_closed_won_date,
    dates_fields_for_opportunities.stage_6_closed_won_month,
    dates_fields_for_opportunities.stage_6_closed_won_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_6_closed_won_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_6_closed_won_fiscal_year,
    dates_fields_for_opportunities.stage_6_closed_lost_date,
    dates_fields_for_opportunities.stage_6_closed_lost_month,
    dates_fields_for_opportunities.stage_6_closed_lost_fiscal_quarter_date,
    dates_fields_for_opportunities.stage_6_closed_lost_fiscal_quarter_name,
    dates_fields_for_opportunities.stage_6_closed_lost_fiscal_year,
    dates_fields_for_opportunities.subscription_start_date,
    dates_fields_for_opportunities.subscription_start_month,
    dates_fields_for_opportunities.subscription_start_fiscal_quarter_date,
    dates_fields_for_opportunities.subscription_start_fiscal_quarter_name,
    dates_fields_for_opportunities.subscription_start_fiscal_year,
    dates_fields_for_opportunities.subscription_end_date,
    dates_fields_for_opportunities.subscription_end_month,
    dates_fields_for_opportunities.subscription_end_fiscal_quarter_date,
    dates_fields_for_opportunities.subscription_end_fiscal_quarter_name,
    dates_fields_for_opportunities.subscription_end_fiscal_year,
    dates_fields_for_opportunities.subscription_renewal_date,
    dates_fields_for_opportunities.sales_qualified_date,
    dates_fields_for_opportunities.sales_qualified_month,
    dates_fields_for_opportunities.sales_qualified_fiscal_quarter_date,
    dates_fields_for_opportunities.sales_qualified_fiscal_quarter_name,
    dates_fields_for_opportunities.sales_qualified_fiscal_year,
    dates_fields_for_opportunities.last_activity_date,
    dates_fields_for_opportunities.last_activity_month,
    dates_fields_for_opportunities.last_activity_fiscal_quarter_date,
    dates_fields_for_opportunities.last_activity_fiscal_quarter_name,
    dates_fields_for_opportunities.last_activity_fiscal_year,
    dates_fields_for_opportunities.sales_last_activity_date,
    dates_fields_for_opportunities.sales_last_activity_month,
    dates_fields_for_opportunities.sales_last_activity_fiscal_quarter_date,
    dates_fields_for_opportunities.sales_last_activity_fiscal_quarter_name,
    dates_fields_for_opportunities.sales_last_activity_fiscal_year,
    dates_fields_for_opportunities.technical_evaluation_date,
    dates_fields_for_opportunities.technical_evaluation_month,
    dates_fields_for_opportunities.technical_evaluation_fiscal_quarter_date,
    dates_fields_for_opportunities.technical_evaluation_fiscal_quarter_name,
    dates_fields_for_opportunities.technical_evaluation_fiscal_year,
    dates_fields_for_opportunities.arr_created_date,
    dates_fields_for_opportunities.arr_created_month,
    dates_fields_for_opportunities.arr_created_fiscal_quarter_date,
    dates_fields_for_opportunities.arr_created_fiscal_quarter_name,
    dates_fields_for_opportunities.arr_created_fiscal_year,
    dates_fields_for_opportunities.pipeline_created_date,
    dates_fields_for_opportunities.pipeline_created_month,
    dates_fields_for_opportunities.pipeline_created_fiscal_quarter_date,
    dates_fields_for_opportunities.pipeline_created_fiscal_quarter_name,
    dates_fields_for_opportunities.pipeline_created_fiscal_year,
    dates_fields_for_opportunities.net_arr_created_date,
    dates_fields_for_opportunities.net_arr_created_month,
    dates_fields_for_opportunities.net_arr_created_fiscal_quarter_date,
    dates_fields_for_opportunities.net_arr_created_fiscal_quarter_name,
    dates_fields_for_opportunities.net_arr_created_fiscal_year,

    fct_crm_opportunity.stage_name,
    fct_crm_opportunity.stage_name_3plus,
    fct_crm_opportunity.stage_name_4plus,
    fct_crm_opportunity.stage_category,
    fct_crm_opportunity.days_in_0_pending_acceptance,
    fct_crm_opportunity.days_in_1_discovery,
    fct_crm_opportunity.days_in_2_scoping,
    fct_crm_opportunity.days_in_3_technical_evaluation,
    fct_crm_opportunity.days_in_4_proposal,
    fct_crm_opportunity.days_in_5_negotiating,
    fct_crm_opportunity.days_in_stage,
    fct_crm_opportunity.cycle_time_in_days,
    fct_crm_opportunity.calculated_age_in_days,
    fct_crm_opportunity.days_in_sao,
    fct_crm_opportunity.days_since_last_activity,
    fct_crm_opportunity.number_of_sa_activity_tasks,

    -- Technical Evalutation fields
    fct_crm_opportunity.sa_tech_evaluation_close_status,
    fct_crm_opportunity.sa_tech_evaluation_end_date,
    fct_crm_opportunity.sa_tech_evaluation_start_date,

    -- Additive metrics
    fct_crm_opportunity.ptc_predicted_arr,
    fct_crm_opportunity.ptc_predicted_renewal_risk_category,
    fct_crm_opportunity.amount,
    fct_crm_opportunity.recurring_amount,
    fct_crm_opportunity.true_up_amount,
    fct_crm_opportunity.proserv_amount,
    fct_crm_opportunity.other_non_recurring_amount,
    fct_crm_opportunity.renewal_amount,
    fct_crm_opportunity.total_contract_value,
    fct_crm_opportunity.calculated_discount,
    fct_crm_opportunity.arr,
    fct_crm_opportunity.arr_basis,
    fct_crm_opportunity.created_arr,
    fct_crm_opportunity.raw_net_arr,
    fct_crm_opportunity.net_arr,
    fct_crm_opportunity.net_arr_stage_1,
    fct_crm_opportunity.enterprise_agile_planning_net_arr,
    fct_crm_opportunity.duo_net_arr,
    fct_crm_opportunity.vsa_start_date_net_arr,
    fct_crm_opportunity.open_1plus_net_arr,
    fct_crm_opportunity.open_3plus_net_arr,
    fct_crm_opportunity.open_4plus_net_arr,
    fct_crm_opportunity.booked_net_arr,
    fct_crm_opportunity.positive_open_net_arr,
    fct_crm_opportunity.positive_booked_net_arr,
    fct_crm_opportunity.closed_net_arr,
    fct_crm_opportunity.created_and_won_same_quarter_net_arr,
    fct_crm_opportunity.new_logo_count,
    fct_crm_opportunity.calculated_deal_count,
    fct_crm_opportunity.open_1plus_deal_count,
    fct_crm_opportunity.open_3plus_deal_count,
    fct_crm_opportunity.open_4plus_deal_count,
    fct_crm_opportunity.booked_deal_count,
    fct_crm_opportunity.positive_booked_deal_count,
    fct_crm_opportunity.positive_open_deal_count,
    fct_crm_opportunity.closed_deals,
    fct_crm_opportunity.closed_won_opps,
    fct_crm_opportunity.closed_opps,
    fct_crm_opportunity.created_deals,
    fct_crm_opportunity.churned_contraction_deal_count,
    fct_crm_opportunity.churned_contraction_net_arr,
    fct_crm_opportunity.booked_churned_contraction_deal_count,
    fct_crm_opportunity.booked_churned_contraction_net_arr,
    fct_crm_opportunity.forecasted_churn_for_clari,
    fct_crm_opportunity.iacv,
    fct_crm_opportunity.net_iacv,
    fct_crm_opportunity.opportunity_based_iacv_to_net_arr_ratio,
    fct_crm_opportunity.segment_order_type_iacv_to_net_arr_ratio,
    fct_crm_opportunity.calculated_from_ratio_net_arr,
    fct_crm_opportunity.weighted_linear_iacv,
    fct_crm_opportunity.closed_buckets,
    fct_crm_opportunity.count_campaigns,
    fct_crm_opportunity.count_crm_attribution_touchpoints,
    fct_crm_opportunity.xdr_net_arr_stage_1,
    fct_crm_opportunity.xdr_net_arr_stage_3,
    fct_crm_opportunity.won_arr_basis_for_clari,
    fct_crm_opportunity.arr_basis_for_clari,
    fct_crm_opportunity.override_arr_basis_clari

  FROM fct_crm_opportunity
  LEFT JOIN dates_fields_for_opportunities 
    ON fct_crm_opportunity.dim_crm_opportunity_id = dates_fields_for_opportunities.dim_crm_opportunity_id
  LEFT JOIN dim_crm_opportunity_flags
    ON fct_crm_opportunity.dim_crm_opportunity_flags_sk = dim_crm_opportunity_flags.dim_crm_opportunity_flags_sk
  LEFT JOIN dim_crm_opportunity_partner
    ON fct_crm_opportunity.dim_crm_opportunity_partner_sk = dim_crm_opportunity_partner.dim_crm_opportunity_partner_sk
  LEFT JOIN dim_crm_command_plan
    ON fct_crm_opportunity.dim_crm_command_plan_sk = dim_crm_command_plan.dim_crm_command_plan_sk
  LEFT JOIN dim_crm_opportunity_source_and_path
    ON fct_crm_opportunity.dim_crm_opportunity_source_and_path_sk = dim_crm_opportunity_source_and_path.dim_crm_opportunity_source_and_path_sk
  LEFT JOIN dim_crm_opportunity_deal
    ON fct_crm_opportunity.dim_crm_opportunity_deal_sk = dim_crm_opportunity_deal.dim_crm_opportunity_deal_sk
  LEFT JOIN dim_crm_opportunity
    ON fct_crm_opportunity.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id
  LEFT JOIN dim_crm_account
    ON fct_crm_opportunity.dim_crm_account_id = dim_crm_account.dim_crm_account_id
  LEFT JOIN dim_crm_user_hierarchy
    ON fct_crm_opportunity.dim_crm_current_account_set_hierarchy_sk = dim_crm_user_hierarchy.dim_crm_user_hierarchy_sk
  LEFT JOIN dim_sales_qualified_source
    ON fct_crm_opportunity.dim_sales_qualified_source_id = dim_sales_qualified_source.dim_sales_qualified_source_id
  LEFT JOIN dim_order_type
    ON fct_crm_opportunity.dim_order_type_id = dim_order_type.dim_order_type_id
  LEFT JOIN dim_order_type AS dim_order_type_current
    ON fct_crm_opportunity.dim_order_type_current_id = dim_order_type_current.dim_order_type_id
  LEFT JOIN dim_alliance_type
    ON fct_crm_opportunity.dim_alliance_type_id = dim_alliance_type.dim_alliance_type_id
  LEFT JOIN dim_channel_type
    ON fct_crm_opportunity.dim_channel_type_id = dim_channel_type.dim_channel_type_id
  LEFT JOIN dim_crm_user
    ON fct_crm_opportunity.dim_crm_user_id = dim_crm_user.dim_crm_user_id
  LEFT JOIN dim_sales_dev_user_hierarchy
    ON fct_crm_opportunity.dim_crm_person_id = dim_sales_dev_user_hierarchy.dim_crm_user_id
    AND dates_fields_for_opportunities.stage_1_discovery_date = dim_sales_dev_user_hierarchy.snapshot_date
  LEFT JOIN dim_crm_account AS partner_account
    ON dim_crm_opportunity_partner.partner_account = partner_account.dim_crm_account_id
    
)


{{ dbt_audit(
    cte_ref="final",
    created_by="@dnel",
    updated_by="@dnel",
    created_date="2025-02-01",
    updated_date="2025-03-06"
  ) }}
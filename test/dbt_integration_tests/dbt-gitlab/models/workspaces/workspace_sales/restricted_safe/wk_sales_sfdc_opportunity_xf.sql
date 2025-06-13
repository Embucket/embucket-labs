{{ config(alias='sfdc_opportunity_xf') }}

WITH edm_opty AS (

    SELECT  *

    --FROM prod.restricted_safe_common_mart_sales.mart_crm_opportunity
    FROM {{ref('wk_sales_mart_crm_opportunity')}} 

), sfdc_users_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), agg_demo_keys_fy23 AS (
-- keys used for aggregated historical analysis

    SELECT *
    FROM {{ ref('wk_sales_report_agg_demo_sqs_ot_keys') }}
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), agg_demo_keys_base AS (

    SELECT *
    FROM {{ ref('wk_sales_report_agg_keys_base') }}
    --FROM restricted_safe_workspace_sales.report_agg_demo_sqs_ot_keys

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM date_details
  WHERE date_actual = CURRENT_DATE

), sfdc_opportunity_xf AS (

   SELECT

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    edm_opty.account_owner_user_segment,
    edm_opty.account_owner_raw_user_segment,
    edm_opty.account_owner_user_geo,
    edm_opty.account_owner_user_region,
    edm_opty.account_owner_user_area,

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    edm_opty.account_owner_user_business_unit,
    edm_opty.account_owner_user_sub_business_unit,
    edm_opty.account_owner_user_division,
    edm_opty.account_owner_user_asm,
    edm_opty.account_owner_user_role_type,

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    edm_opty.opportunity_owner_user_business_unit,
    edm_opty.opportunity_owner_user_sub_business_unit,
    edm_opty.opportunity_owner_user_division,
    edm_opty.opportunity_owner_user_asm,
    edm_opty.opportunity_owner_user_role_type,

    edm_opty.opportunity_owner_user_segment,
    edm_opty.opportunity_owner_raw_user_segment,
    edm_opty.opportunity_owner_user_geo,
    edm_opty.opportunity_owner_user_region,
    edm_opty.opportunity_owner_user_area,

    -- NF: adjusted to FY24 GTM structure
    edm_opty.report_opportunity_user_segment,

    -- NF: unadjusted version of segment used to create the FY24 GTM key
    edm_opty.report_opportunity_raw_user_segment,

    edm_opty.report_opportunity_user_geo,
    edm_opty.report_opportunity_user_region,
    edm_opty.report_opportunity_user_area,

    -- NF 20230214
    -- FY24 GTM calculated fields. These fields will be sourced from EDM eventually
    edm_opty.report_opportunity_user_business_unit,
    edm_opty.report_opportunity_user_sub_business_unit,
    edm_opty.report_opportunity_user_division,
    edm_opty.report_opportunity_user_asm,
    edm_opty.report_opportunity_user_role_type,

    edm_opty.partner_category,
    edm_opty.alliance_partner,

    ------------------------------------------------------------------------
    -- creating report_user_segment_geo_region_area_sqs_ot with adjusted segment
    edm_opty.report_user_segment_geo_region_area_sqs_ot,
    edm_opty.report_user_segment_geo_region_area,
    ------------------------------------------------------------------------
    -- FY24 keys
    edm_opty.report_bu_user_segment_geo_region_area_sqs_ot,
    edm_opty.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot,
    edm_opty.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap,

    ----------------------------------------------------------
    ----------------------------------------------------------

    edm_opty.dbt_updated_at                         AS _last_dbt_run,
    edm_opty.dim_crm_account_id                     AS account_id,
    edm_opty.dim_crm_opportunity_id                 AS opportunity_id,
    edm_opty.opportunity_name                       AS opportunity_name,

    edm_opty.close_date                             AS close_date,
    edm_opty.created_date                           AS created_date,
    edm_opty.sales_accepted_date,
    edm_opty.sales_qualified_date,
    edm_opty.subscription_start_date                AS quote_start_date,
    edm_opty.subscription_end_date                  AS quote_end_date,

    edm_opty.days_in_stage,
    edm_opty.deployment_preference,
    edm_opty.merged_crm_opportunity_id,
    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.owner_id,
    -- edm_opty.opportunity_owner,
    opportunity_owner.name                          AS opportunity_owner,
    edm_opty.opportunity_owner_department,
    edm_opty.opportunity_owner_manager,
    edm_opty.opportunity_owner_role,
    edm_opty.opportunity_owner_title,
    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.opportunity_term,
    edm_opty.primary_campaign_source_id            AS primary_campaign_source_id,
    edm_opty.sales_path                            AS sales_path,
    edm_opty.subscription_type                     AS sales_type,
    edm_opty.subscription_type                     AS subscription_type,
    edm_opty.stage_name                            AS stage_name,
    edm_opty.order_type                            AS order_type_stamped,
    edm_opty.order_type_current                    AS order_type_current,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Amount fields
    COALESCE(edm_opty.net_arr,0)                   AS raw_net_arr,
    edm_opty.net_arr,
    edm_opty.amount,
    edm_opty.renewal_amount,
    edm_opty.recurring_amount,
    edm_opty.true_up_amount,
    edm_opty.proserv_amount,
    edm_opty.other_non_recurring_amount,
    edm_opty.arr_basis,
    edm_opty.arr,
    
    -- ATR for Booked Churned / Contraction metrics
    edm_opty.atr,
    edm_opty.won_atr,
    edm_opty.churned_contraction_net_arr,
    edm_opty.booked_churned_contraction_net_arr,

    edm_opty.competitors,
    edm_opty.fpa_master_bookings_flag,
    edm_opty.forecast_category_name,
    edm_opty.invoice_number,
    edm_opty.professional_services_value,
    edm_opty.is_ps_opp,
    edm_opty.reason_for_loss,
    edm_opty.reason_for_loss_details,
    edm_opty.downgrade_reason,
    edm_opty.lead_source,

    edm_opty.is_downgrade,
    edm_opty.is_edu_oss,
    edm_opty.solutions_to_be_replaced,
    edm_opty.total_contract_value,
    edm_opty.is_web_portal_purchase,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Support Team Members
    edm_opty.crm_business_dev_rep_id,
    edm_opty.crm_sales_dev_rep_id,
    edm_opty.opportunity_development_representative,
    -- Missing ISR & TAM

    ----------------------------------------------------------
    ----------------------------------------------------------
    edm_opty.opportunity_health,
    edm_opty.is_risky,
    edm_opty.risk_type,
    edm_opty.risk_reasons,
    edm_opty.tam_notes,
    edm_opty.days_in_1_discovery,
    edm_opty.days_in_2_scoping,
    edm_opty.days_in_3_technical_evaluation,
    edm_opty.days_in_4_proposal,
    edm_opty.days_in_5_negotiating,
    edm_opty.stage_0_pending_acceptance_date,
    edm_opty.stage_1_discovery_date,
    edm_opty.stage_2_scoping_date,
    edm_opty.stage_3_technical_evaluation_date,
    edm_opty.stage_4_proposal_date,
    edm_opty.stage_5_negotiating_date,
    edm_opty.stage_6_awaiting_signature_date_date         AS stage_6_awaiting_signature_date,
    edm_opty.stage_6_closed_won_date,
    edm_opty.stage_6_closed_lost_date,
    edm_opty.cp_champion,
    edm_opty.cp_close_plan,
    edm_opty.cp_decision_criteria,
    edm_opty.cp_decision_process,
    edm_opty.cp_economic_buyer,
    edm_opty.cp_identify_pain,
    edm_opty.cp_metrics,
    edm_opty.cp_risks,
    edm_opty.cp_use_cases,
    edm_opty.cp_value_driver,
    edm_opty.cp_why_do_anything_at_all,
    edm_opty.cp_why_gitlab,
    edm_opty.cp_why_now,
    edm_opty.cp_score,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- fields form opportunity source
    edm_opty.opportunity_category,
    edm_opty.product_category,

    ----------------------------------------------------------
    ----------------------------------------------------------
    -- Channel Org. fields
    edm_opty.deal_path_name                             AS deal_path,
    edm_opty.dr_partner_deal_type,
    edm_opty.dr_partner_engagement_name                 AS dr_partner_engagement, 
    edm_opty.partner_account,
    edm_opty.partner_account_name,
    edm_opty.dr_status,
    edm_opty.distributor,
    edm_opty.influence_partner,
    edm_opty.partner_track,
    edm_opty.partner_gitlab_program,
    edm_opty.is_public_sector_opp,
    edm_opty.is_registration_from_portal,
    edm_opty.calculated_discount,
    edm_opty.partner_discount,
    edm_opty.partner_discount_calc,
    edm_opty.comp_channel_neutral,
    edm_opty.fulfillment_partner                       AS resale_partner_id,
    resale_account.account_name                        AS resale_partner_name,
    edm_opty.platform_partner,

    ----------------------------------------------------------
    ----------------------------------------------------------

    -- account driven fields
    edm_opty.crm_account_name                          AS account_name,
    edm_opty.dim_parent_crm_account_id                 AS ultimate_parent_account_id,
    edm_opty.is_jihu_account,

    account_owner.name                                 AS account_owner_name,
    account_owner.user_id                              AS account_owner_id,                                               

    edm_opty.parent_crm_account_sales_segment,
    edm_opty.parent_crm_account_geo,
    edm_opty.parent_crm_account_region,
    edm_opty.parent_crm_account_area,
    edm_opty.parent_crm_account_territory,

    edm_opty.sales_qualified_source_name               AS sales_qualified_source,
    edm_opty.stage_category,
    edm_opty.calculated_partner_track,
    edm_opty.deal_path_engagement,
    edm_opty.is_refund,
    edm_opty.is_credit                                 AS is_credit_flag,
    edm_opty.is_contract_reset                         AS is_contract_reset_flag,
    edm_opty.is_net_arr_pipeline_created,
    CAST(edm_opty.is_won AS INTEGER)                   AS is_won,
    edm_opty.is_lost,
    edm_opty.is_open,
    edm_opty.is_duplicate                              AS is_duplicate_flag,    
    CASE edm_opty.is_closed 
      WHEN TRUE THEN 1 
      ELSE 0 
    END                                                AS is_closed,
    edm_opty.is_closed                                 AS stage_is_closed,
    edm_opty.is_active                                 AS stage_is_active,
    edm_opty.is_renewal,


    -- date fields helpers -- revisit
    edm_opty.close_fiscal_quarter_name,
    edm_opty.close_fiscal_quarter_date,
    edm_opty.close_fiscal_year,
    edm_opty.close_month                                                 AS close_date_month,

    edm_opty.created_fiscal_quarter_name,
    edm_opty.created_fiscal_quarter_date,
    edm_opty.created_fiscal_year,
    edm_opty.created_month                                               AS created_date_month,

    edm_opty.subscription_start_fiscal_quarter_name                      AS quote_start_date_fiscal_quarter_name,
    edm_opty.subscription_start_fiscal_quarter_date                      AS quote_start_date_fiscal_quarter_date,
    edm_opty.subscription_start_fiscal_year                              AS quote_start_date_fiscal_year,
    edm_opty.subscription_start_month                                    AS quote_start_date_month,

    edm_opty.sales_accepted_fiscal_quarter_name,
    edm_opty.sales_accepted_fiscal_quarter_date,
    edm_opty.sales_accepted_fiscal_year,
    edm_opty.sales_accepted_month                                        AS sales_accepted_date_month,

    edm_opty.sales_qualified_fiscal_quarter_name,
    edm_opty.sales_qualified_fiscal_quarter_date,
    edm_opty.sales_qualified_fiscal_year,
    edm_opty.sales_qualified_month                                       AS sales_qualified_date_month,

    edm_opty.net_arr_created_date,
    edm_opty.net_arr_created_fiscal_quarter_name,
    edm_opty.net_arr_created_fiscal_quarter_date,
    edm_opty.net_arr_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS net_arr_created_date_month,

    edm_opty.pipeline_created_date,
    edm_opty.pipeline_created_fiscal_quarter_name,
    edm_opty.pipeline_created_fiscal_quarter_date,
    edm_opty.pipeline_created_fiscal_year,
    edm_opty.net_arr_created_month                                       AS pipeline_created_date_month,

    edm_opty.stage_1_discovery_date                                      AS stage_1_date,
    edm_opty.stage_1_discovery_month                                     AS stage_1_date_month,
    edm_opty.stage_1_discovery_fiscal_year                               AS stage_1_fiscal_year,
    edm_opty.stage_1_discovery_fiscal_quarter_name                       AS stage_1_fiscal_quarter_name,
    edm_opty.stage_1_discovery_fiscal_quarter_date                       AS stage_1_fiscal_quarter_date,

    edm_opty.stage_3_technical_evaluation_date                           AS stage_3_date,
    edm_opty.stage_3_technical_evaluation_month                          AS stage_3_date_month,
    edm_opty.stage_3_technical_evaluation_fiscal_year                    AS stage_3_fiscal_year,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_name            AS stage_3_fiscal_quarter_name,
    edm_opty.stage_3_technical_evaluation_fiscal_quarter_date            AS stage_3_fiscal_quarter_date,

    -- Last Activity Date <- This date can be in the future, it represents the date of the last activity taken or scheduled 
    edm_opty.last_activity_date,
    edm_opty.last_activity_fiscal_year,
    edm_opty.last_activity_fiscal_quarter_name,
    edm_opty.last_activity_fiscal_quarter_date,
    edm_opty.last_activity_month                                         AS last_activity_date_month,

    -- Sales Activity Date <- Last time an activity was taken against the opportunity
    edm_opty.sales_last_activity_date,
    edm_opty.sales_last_activity_fiscal_year,
    edm_opty.sales_last_activity_fiscal_quarter_name,
    edm_opty.sales_last_activity_fiscal_quarter_date,
    edm_opty.sales_last_activity_month                                   AS sales_last_activity_date_month,

    edm_opty.competitors_other_flag,
    edm_opty.competitors_gitlab_core_flag,
    edm_opty.competitors_none_flag,
    edm_opty.competitors_github_enterprise_flag,
    edm_opty.competitors_bitbucket_server_flag,
    edm_opty.competitors_unknown_flag,
    edm_opty.competitors_github_flag,
    edm_opty.competitors_gitlab_flag,
    edm_opty.competitors_jenkins_flag,
    edm_opty.competitors_azure_devops_flag,
    edm_opty.competitors_svn_flag,
    edm_opty.competitors_bitbucket_flag,
    edm_opty.competitors_atlassian_flag,
    edm_opty.competitors_perforce_flag,
    edm_opty.competitors_visual_studio_flag,
    edm_opty.competitors_azure_flag,
    edm_opty.competitors_amazon_code_commit_flag,
    edm_opty.competitors_circleci_flag,
    edm_opty.competitors_bamboo_flag,
    edm_opty.competitors_aws_flag,
    edm_opty.is_comp_new_logo_override,
    edm_opty.is_stage_1_plus,
    edm_opty.is_stage_3_plus,
    edm_opty.is_stage_4_plus,
    edm_opty.stage_name_3plus,
    edm_opty.stage_name_4plus,
    edm_opty.deal_category,
    edm_opty.deal_group,
    edm_opty.calculated_deal_count                                   AS calculated_deal_count,
    edm_opty.lam_dev_count,

    ----------------------------------------------------------------
    -- NF 2022-01-28 This is probably TO BE DEPRECATED too, need to align with Channel ops
    -- PIO Flag for PIO reporting dashboard
    CASE
    WHEN edm_opty.dr_partner_engagement = 'PIO'
        THEN 1
    ELSE 0
    END                                                             AS partner_engaged_opportunity_flag,

    -- check if renewal was closed on time or not
    CASE
    WHEN LOWER(edm_opty.subscription_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   >= edm_opty.close_fiscal_quarter_date
        THEN 'On-Time'
    WHEN LOWER(edm_opty.subscription_type) like '%renewal%'
        AND start_date.first_day_of_fiscal_quarter   < edm_opty.close_fiscal_quarter_date
        THEN 'Late'
    END                                                            AS renewal_timing_status,

    ----------------------------------------------------------------
    ----------------------------------------------------------------
    -- calculated fields for pipeline velocity report

    -- 20201021 NF: This should be replaced by a table that keeps track of excluded deals for forecasting purposes
    edm_opty.is_excluded_from_pipeline_created                     AS is_excluded_flag,
    -----------------------------------------------

    ---- measures
    edm_opty.open_1plus_deal_count,
    edm_opty.open_3plus_deal_count,
    edm_opty.open_4plus_deal_count,
    edm_opty.booked_deal_count,
    edm_opty.churned_contraction_deal_count,
    edm_opty.booked_churned_contraction_deal_count,
    edm_opty.open_1plus_net_arr,
    edm_opty.open_3plus_net_arr,
    edm_opty.open_4plus_net_arr,
    edm_opty.booked_net_arr,
   
    edm_opty.deal_size,
    edm_opty.calculated_deal_size,

    ---------------------------------------------
    ---------------------------------------------

    -- NF: calculated age only considers created date to close date or actual date if open
    edm_opty.calculated_age_in_days,

    -- NF: cycle time will consider if the opty is renewal and eligible to be considered 
    -- using the is_eligible_cycle_time_analysis
    CASE 
        WHEN edm_opty.is_edu_oss = 0
            AND edm_opty.is_deleted = 0
            -- For stage age we exclude only ps/other
            AND edm_opty.order_type IN ('1. New - First Order','2. New - Connected','3. Growth','4. Contraction','6. Churn - Final','5. Churn - Partial')
            -- Only include deal types with meaningful journeys through the stages
            AND edm_opty.opportunity_category IN ('Standard')
            -- Web Purchase have a different dynamic and should not be included
            AND edm_opty.is_web_portal_purchase = 0
                THEN 1
        ELSE 0
    END                                                           AS is_eligible_cycle_time_analysis_flag,

    -- NF: We consider net_arr_created date for renewals as they haver a very distinct motion than 
    --      add on and First Orders
    --     Logic is different for open deals so we can evaluate their current cycle time.
    CASE
        WHEN edm_opty.is_renewal = 1 AND is_closed = 1
            THEN DATEDIFF(day, edm_opty.net_arr_created_date, edm_opty.close_date)
        WHEN edm_opty.is_renewal = 0 AND is_closed = 1
            THEN DATEDIFF(day, edm_opty.created_date, edm_opty.close_date)
         WHEN edm_opty.is_renewal = 1 AND is_open = 1
            THEN DATEDIFF(day, edm_opty.net_arr_created_date, CURRENT_DATE)
        WHEN edm_opty.is_renewal = 0 AND is_open = 1
            THEN DATEDIFF(day, edm_opty.created_date, CURRENT_DATE)
    END                                                           AS cycle_time_in_days,

    -- For some analysis it is important to order stages by rank
    CASE
            WHEN edm_opty.stage_name = '0-Pending Acceptance'
                THEN 0
            WHEN edm_opty.stage_name = '1-Discovery'
                THEN 1
             WHEN edm_opty.stage_name = '2-Scoping'
                THEN 2
            WHEN edm_opty.stage_name = '3-Technical Evaluation'
                THEN 3
            WHEN edm_opty.stage_name = '4-Proposal'
                THEN 4
            WHEN edm_opty.stage_name = '5-Negotiating'
                THEN 5
            WHEN edm_opty.stage_name = '6-Awaiting Signature'
                THEN 6
            WHEN edm_opty.stage_name = '7-Closing'
                THEN 7
            WHEN edm_opty.stage_name = 'Closed Won'
                THEN 8
            WHEN edm_opty.stage_name = '8-Closed Lost'
                THEN 9
            WHEN edm_opty.stage_name = '9-Unqualified'
                THEN 10
            WHEN edm_opty.stage_name = '10-Duplicate'
                THEN 11
            ELSE NULL
    END                     AS stage_name_rank,

    CASE
        WHEN edm_opty.stage_name IN ('0-Pending Acceptance')
            THEN '0. Acceptance' 
         WHEN edm_opty.stage_name IN ('1-Discovery','2-Scoping')
            THEN '1. Early'
         WHEN edm_opty.stage_name IN ('3-Technical Evaluation','4-Proposal')
            THEN '2. Middle'
         WHEN edm_opty.stage_name IN ('5-Negotiating','6-Awaiting Signature')
            THEN '3. Late'
        ELSE '4. Closed'
    END                     AS pipeline_category,

    CASE
        WHEN DATEDIFF(MONTH, edm_opty.pipeline_created_fiscal_quarter_date, edm_opty.close_fiscal_quarter_date) < 3
            THEN 'CQ'
        WHEN DATEDIFF(MONTH, edm_opty.pipeline_created_fiscal_quarter_date, edm_opty.close_fiscal_quarter_date) < 6
            THEN 'CQ+1'
        WHEN DATEDIFF(MONTH, edm_opty.pipeline_created_fiscal_quarter_date, edm_opty.close_fiscal_quarter_date) < 9
            THEN 'CQ+2'
        WHEN DATEDIFF(MONTH, edm_opty.pipeline_created_fiscal_quarter_date, edm_opty.close_fiscal_quarter_date) < 12
            THEN 'CQ+3'
        WHEN DATEDIFF(MONTH, edm_opty.pipeline_created_fiscal_quarter_date, edm_opty.close_fiscal_quarter_date) >= 12
            THEN 'CQ+4 >'
    END                                         AS pipeline_landing_quarter,

    ---------------------------------------------
    ---------------------------------------------

    edm_opty.is_eligible_open_pipeline              AS is_eligible_open_pipeline_flag,
    edm_opty.is_eligible_asp_analysis               AS is_eligible_asp_analysis_flag,
    edm_opty.is_eligible_age_analysis               AS is_eligible_age_analysis_flag,
    edm_opty.is_booked_net_arr                      AS is_booked_net_arr_flag,
    edm_opty.is_eligible_churn_contraction          AS is_eligible_churn_contraction_flag,
    edm_opty.created_and_won_same_quarter_net_arr,
    edm_opty.churn_contraction_net_arr_bucket,
    edm_opty.reason_for_loss_calc,    
    CASE edm_opty.is_sao 
      WHEN TRUE THEN 1 
      ELSE 0 
    END                                             AS is_eligible_sao_flag,
    edm_opty.is_deleted,
    opportunity_owner.is_rep_flag,
    edm_opty.pushed_count,
    edm_opty.intented_product_tier,

    -- to simplify reporting, we adjust parent opportunity to default to the opportunity id when null
    CASE
        WHEN edm_opty.parent_opportunity IS NULL
            THEN edm_opty.dim_crm_opportunity_id
        ELSE edm_opty.parent_opportunity
    END  AS parent_opportunity,

    -- Calculated fields
    CASE
        WHEN LOWER(edm_opty.product_category) LIKE '%premium%'
            THEN 'Premium'
        WHEN LOWER(edm_opty.product_category) LIKE '%ultimate%'
            THEN 'Ultimate'
        WHEN LOWER(edm_opty.intented_product_tier) LIKE '%premium%'
            THEN 'Premium'
        WHEN LOWER(edm_opty.intented_product_tier) LIKE '%ultimate%'
            THEN 'Ultimate'
        ELSE 'Other'
    END AS  product_category_tier,

    CASE
        WHEN lower(edm_opty.product_category) LIKE '%saas%'
                THEN 'SaaS'
        WHEN lower(edm_opty.product_category) LIKE '%self-managed%'
                THEN 'Self-Managed'
        ELSE 'Other'
    END AS  product_category_deployment,

    -- 
    CASE
        WHEN cycle_time_in_days BETWEEN 0 AND 29
            THEN '[0,30)'
        WHEN cycle_time_in_days BETWEEN 30 AND 179
            THEN '[30,180)'
        WHEN cycle_time_in_days BETWEEN 180 AND 364
            THEN '[180,365)'
        WHEN cycle_time_in_days > 364
            THEN '[365+)'
        ELSE 'N/A'
    END                  AS age_bin,

    -- age in stage
    CASE
        WHEN edm_opty.stage_name = '0-Pending Acceptance'
            THEN edm_opty.created_date
        WHEN edm_opty.stage_name = '1-Discovery'
            THEN edm_opty.stage_1_discovery_date
        WHEN edm_opty.stage_name = '2-Scoping'
            THEN edm_opty.stage_2_scoping_date
        WHEN edm_opty.stage_name = '3-Technical Evaluation'
            THEN edm_opty.stage_3_technical_evaluation_date
        WHEN edm_opty.stage_name = '4-Proposal'
            THEN edm_opty.stage_4_proposal_date
        WHEN edm_opty.stage_name = '5-Negotiating'
            THEN edm_opty.stage_5_negotiating_date
        WHEN edm_opty.stage_name = '6-Awaiting Signature'
            THEN edm_opty.stage_6_awaiting_signature_date_date
        WHEN edm_opty.stage_name = '7-Closing'
            THEN edm_opty.close_date
        WHEN edm_opty.stage_name = '8-Closed Lost'
            THEN edm_opty.close_date
        WHEN edm_opty.stage_name = '9-Unqualified'
            THEN edm_opty.close_date
        WHEN edm_opty.stage_name = '10-Duplicate'
            THEN edm_opty.close_date
        WHEN edm_opty.stage_name = 'Closed Won'
            THEN edm_opty.close_date
    END                                 AS current_stage_start_date,

    CASE 
        WHEN is_open = 1 AND current_stage_start_date < CURRENT_DATE()
            THEN DATEDIFF(DAY,current_stage_start_date,CURRENT_DATE())
        WHEN current_stage_start_date < close_date
            THEN DATEDIFF(DAY,current_stage_start_date,close_date)
        ELSE NULL
    END                                 AS current_stage_age,

    CASE
        WHEN current_stage_age BETWEEN 0 AND 29
            THEN '[0,30)'
        WHEN current_stage_age BETWEEN 30 AND 179
            THEN '[30,180)'
        WHEN current_stage_age BETWEEN 180 AND 364
            THEN '[180,365)'
        WHEN current_stage_age > 364
            THEN '[365+)'
        ELSE 'N/A'
    END                  AS current_stage_age_bin,

    -- demographics fields
    edm_opty.parent_crm_account_upa_country,
    edm_opty.parent_crm_account_upa_state,
    edm_opty.parent_crm_account_upa_city,
    edm_opty.parent_crm_account_upa_street,
    edm_opty.parent_crm_account_upa_postal_code,
    account.parent_crm_account_upa_country_name,
    account.industry,
    edm_opty.parent_crm_account_business_unit,

    -- account driven fields
    account.lam_dev_count_bin
    
    FROM edm_opty
    -- Date helpers
    INNER JOIN sfdc_accounts_xf AS account
      ON account.account_id = edm_opty.dim_crm_account_id
    INNER JOIN sfdc_users_xf AS account_owner
      ON account_owner.user_id = account.owner_id
    INNER JOIN date_details AS created_date_detail
      ON created_date_detail.date_actual = edm_opty.created_date::DATE
    INNER JOIN sfdc_users_xf AS opportunity_owner
      ON opportunity_owner.user_id = edm_opty.owner_id
    LEFT JOIN date_details AS start_date
      ON edm_opty.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN sfdc_accounts_xf AS resale_account
      ON resale_account.account_id = edm_opty.fulfillment_partner
    -- NF 20210906 remove JiHu opties from the models
    WHERE edm_opty.is_jihu_account = 0
        AND account.ultimate_parent_account_id NOT IN ('0016100001YUkWVAA1')            -- remove test account
        AND edm_opty.dim_crm_account_id NOT IN ('0014M00001kGcORQA0')                -- remove test account
        AND edm_opty.is_deleted = 0


), churn_metrics AS (

    SELECT
        o.opportunity_id,
        NVL(o.reason_for_loss, o.downgrade_reason) AS reason_for_loss_staged,
        o.reason_for_loss_details,

        CASE
          WHEN o.order_type_stamped IN ('4. Contraction','5. Churn - Partial')
            THEN 'Contraction'
          ELSE 'Churn'
        END                                    AS churn_contraction_type_calc

    FROM sfdc_opportunity_xf o
    WHERE o.order_type_stamped IN ('4. Contraction','5. Churn - Partial','6. Churn - Final')
        AND (o.is_won = 1
            OR (is_renewal = 1 AND is_lost = 1))

), service_opportunities AS (

    SELECT
        CASE
            WHEN parent_opportunity IS NULL
                THEN opportunity_id
            ELSE parent_opportunity
            END  AS opportunity_id,
        COUNT(opportunity_id)               AS count_service_opportunities,
        SUM(professional_services_value)    AS total_professional_services_value,
        SUM(CASE
                WHEN is_won = 1
                    THEN professional_services_value
                ELSE 0
            END)                            AS total_book_professional_services_value,
       SUM(CASE
                WHEN is_lost = 1
                    THEN professional_services_value
                ELSE 0
            END)                            AS total_lost_professional_services_value,
       SUM(CASE
                WHEN is_open = 1
                    THEN professional_services_value
                ELSE 0
            END)                            AS total_open_professional_services_value
    FROM sfdc_opportunity_xf
    WHERE professional_services_value <> 0
    GROUP BY 1

), oppty_final AS (

    SELECT
        sfdc_opportunity_xf.*,

        -- Customer Success related fields
        -- DRI Michael Armtz
        churn_metrics.reason_for_loss_staged,
        -- churn_metrics.reason_for_loss_calc, -- part of edm opp mart
        churn_metrics.churn_contraction_type_calc,

        --services total amount
        COALESCE(service_opportunities.total_professional_services_value,0) AS total_professional_services_value,
      
        COALESCE(service_opportunities.total_book_professional_services_value,0) AS total_book_professional_services_value,
        COALESCE(service_opportunities.total_lost_professional_services_value,0) AS total_lost_professional_services_value,
        COALESCE(service_opportunities.total_open_professional_services_value,0) AS total_open_professional_services_value

    FROM sfdc_opportunity_xf
    CROSS JOIN today
    LEFT JOIN churn_metrics
      ON churn_metrics.opportunity_id = sfdc_opportunity_xf.opportunity_id
    LEFT JOIN service_opportunities 
      ON service_opportunities.opportunity_id = sfdc_opportunity_xf.opportunity_id

), add_calculated_net_arr_to_opty_final AS (

    SELECT
      oppty_final.*,


      -- JK 2023-02-06: FY23 keys for temp dashboard solution until tools are ready for FY24 keys 
      -- NF 2022-02-17 These keys are used in the pipeline metrics models and on the X-Ray dashboard to link gSheets with
      -- different aggregation levels
      LOWER(agg_demo_keys_fy23.key_sqs)                             AS key_sqs,
      LOWER(agg_demo_keys_fy23.key_ot)                              AS key_ot,
      LOWER(agg_demo_keys_fy23.key_segment)                         AS key_segment,
      LOWER(agg_demo_keys_fy23.key_segment_sqs)                     AS key_segment_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_ot)                      AS key_segment_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo)                     AS key_segment_geo,
      LOWER(agg_demo_keys_fy23.key_segment_geo_sqs)                 AS key_segment_geo_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_ot)                  AS key_segment_geo_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region)              AS key_segment_geo_region,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_sqs)          AS key_segment_geo_region_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_ot)           AS key_segment_geo_region_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area)         AS key_segment_geo_region_area,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area_sqs)     AS key_segment_geo_region_area_sqs,
      LOWER(agg_demo_keys_fy23.key_segment_geo_region_area_ot)      AS key_segment_geo_region_area_ot,
      LOWER(agg_demo_keys_fy23.key_segment_geo_area)                AS key_segment_geo_area,
      LOWER(agg_demo_keys_fy23.sales_team_cro_level)                AS sales_team_cro_level,
      LOWER(agg_demo_keys_fy23.sales_team_rd_asm_level)             AS sales_team_rd_asm_level,
      LOWER(agg_demo_keys_fy23.sales_team_vp_level)                 AS sales_team_vp_level,
      LOWER(agg_demo_keys_fy23.sales_team_avp_rd_level)             AS sales_team_avp_rd_level,
      LOWER(agg_demo_keys_fy23.sales_team_asm_level)                AS sales_team_asm_level,

      -- JK 2023-02-06: FY24 keys
      LOWER(agg_demo_keys_base.key_bu)                      AS key_bu,
      LOWER(agg_demo_keys_base.key_bu_ot)                   AS key_bu_ot,
      LOWER(agg_demo_keys_base.key_bu_sqs)                  AS key_bu_sqs,
      LOWER(agg_demo_keys_base.key_bu_subbu)                AS key_bu_subbu,
      LOWER(agg_demo_keys_base.key_bu_subbu_ot)             AS key_bu_subbu_ot,
      LOWER(agg_demo_keys_base.key_bu_subbu_sqs)            AS key_bu_subbu_sqs,
      LOWER(agg_demo_keys_base.key_bu_subbu_division)       AS key_bu_subbu_division,
      LOWER(agg_demo_keys_base.key_bu_subbu_division_ot)    AS key_bu_subbu_division_ot,
      LOWER(agg_demo_keys_base.key_bu_subbu_division_sqs)   AS key_bu_subbu_division_sqs,
      LOWER(agg_demo_keys_base.key_bu_subbu_division_asm)   AS key_bu_subbu_division_asm,


        --NF 2024-01-31: FY25 keys
        agg_demo_keys_base.key_geo,

        agg_demo_keys_base.key_geo_ot,
        agg_demo_keys_base.key_geo_sqs,
            
        agg_demo_keys_base.key_geo_bu,
        agg_demo_keys_base.key_geo_bu_ot,
        agg_demo_keys_base.key_geo_bu_sqs,

        agg_demo_keys_base.key_geo_bu_region,
        agg_demo_keys_base.key_geo_bu_region_ot,
        agg_demo_keys_base.key_geo_bu_region_sqs,

        agg_demo_keys_base.key_geo_bu_region_area,
        agg_demo_keys_base.key_geo_bu_region_area_segment,


      -- Created pipeline eligibility definition
      -- https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/2389
      CASE
        WHEN oppty_final.order_type_stamped IN ('1. New - First Order' ,'2. New - Connected', '3. Growth')
          AND oppty_final.is_edu_oss = 0
          AND oppty_final.pipeline_created_fiscal_quarter_date IS NOT NULL
          AND oppty_final.opportunity_category IN ('Standard','Internal Correction','Ramp Deal','Credit','Contract Reset')
          -- 20211222 Adjusted to remove the ommitted filter
          AND oppty_final.stage_name NOT IN ('00-Pre Opportunity','10-Duplicate', '9-Unqualified','0-Pending Acceptance')
          AND (net_arr > 0
            OR oppty_final.opportunity_category = 'Credit')
          -- 20220128 Updated to remove webdirect SQS deals
          AND oppty_final.sales_qualified_source  != 'Web Direct Generated'
          AND oppty_final.is_jihu_account = 0
         THEN 1
         ELSE 0
      END                                                          AS is_eligible_created_pipeline_flag


    FROM oppty_final
    -- Add keys for aggregated analysis
    LEFT JOIN agg_demo_keys_fy23
      ON oppty_final.report_user_segment_geo_region_area_sqs_ot = agg_demo_keys_fy23.report_user_segment_geo_region_area_sqs_ot
    LEFT JOIN agg_demo_keys_base
      ON oppty_final.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap = agg_demo_keys_base.report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap


)
SELECT *
FROM add_calculated_net_arr_to_opty_final

{{ config(alias='mart_crm_opportunity') }}

WITH sfdc_users_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_users_xf')}}
    --FROM prod.workspace_sales.sfdc_users_xf

), sfdc_accounts_xf AS (

    SELECT *
    FROM {{ref('wk_sales_sfdc_accounts_xf')}}
    -- FROM PROD.restricted_safe_workspace_sales.sfdc_accounts_xf

), sfdc_opportunity_raw AS (


    SELECT *
    FROM {{ source('salesforce', 'opportunity') }}  AS opportunity


), sfdc_opportunity_source AS (

    SELECT source.*,
        raw.intended_product_tier__c AS intented_product_tier,
        raw.parent_opportunity__c   AS parent_opportunity

    FROM {{ref('sfdc_opportunity_source')}} source
        LEFT JOIN sfdc_opportunity_raw raw
            ON source.opportunity_id = raw.id

), date_details AS (

    SELECT *
    FROM {{ ref('wk_sales_date_details') }}
    --FROM prod.workspace_sales.date_details

), today AS (

  SELECT DISTINCT
    fiscal_year               AS current_fiscal_year,
    first_day_of_fiscal_year  AS current_fiscal_year_date
  FROM date_details
  WHERE date_actual = CURRENT_DATE

), edm_opty AS (

    SELECT  
       dim_crm_opportunity_id,
       dim_parent_crm_account_id,
       dim_crm_user_id,
       duplicate_opportunity_id,
       merged_crm_opportunity_id,
       record_type_id,
       ssp_id,
       dim_crm_account_id,
       opportunity_name,
       stage_name,
       reason_for_loss,
       reason_for_loss_details,
       reason_for_loss_staged,
       reason_for_loss_calc,
       risk_type,
       risk_reasons,
       downgrade_reason,
       subscription_type     AS sales_type,
       subscription_type,
       closed_buckets,
       opportunity_category,
       source_buckets,
       crm_sales_dev_rep_id,
       crm_business_dev_rep_id,
       opportunity_development_representative,
       sdr_or_bdr,
       iqm_submitted_by_role,
       sdr_pipeline_contribution,
       fpa_master_bookings_flag,
       sales_path,
       professional_services_value,
       primary_solution_architect,
       product_details,
       product_category,
       products_purchased,
       growth_type,
       opportunity_deal_size,
       primary_campaign_source_id,
       ga_client_id,
       deployment_preference,
       net_new_source_categories,
       invoice_number,
       opportunity_term,
       stage_name_3plus,
       stage_name_4plus,
       stage_category,
       deal_category,
       deal_group,
       deal_size,
       calculated_deal_size,
       dr_partner_engagement,
       deal_path_engagement,
       forecast_category_name,
       opportunity_owner,
       opportunity_owner_manager,
       opportunity_owner_department,
       opportunity_owner_role,
       opportunity_owner_title,
       solutions_to_be_replaced,
       opportunity_health,
       tam_notes,
       generated_source,
       churn_contraction_type,
       churn_contraction_net_arr_bucket,
       owner_id,
       resale_partner_name,
       deal_path_name,
       order_type,
       order_type_grouped,
       order_type_current,
       dr_partner_engagement_name,
       alliance_type_name,
       alliance_type_short_name,
       channel_type_name,
       sales_qualified_source_name,
       sales_qualified_source_grouped,
       sqs_bucket_engagement,
       record_type_name,
       crm_account_name,
       parent_crm_account_name,
       parent_crm_account_sales_segment,
       parent_crm_account_geo,
       parent_crm_account_region,
       parent_crm_account_area,
       parent_crm_account_territory,
       parent_crm_account_max_family_employee,
       parent_crm_account_upa_country,
       parent_crm_account_upa_state,
       parent_crm_account_upa_city,
       parent_crm_account_upa_street,
       parent_crm_account_upa_postal_code,
       parent_crm_account_business_unit,
       parent_crm_account_role_type,
       crm_account_employee_count,
       crm_account_gtm_strategy,
       crm_account_focus_account,
       crm_account_zi_technologies,
       is_jihu_account,
       admin_manual_source_number_of_employees,
       admin_manual_source_account_address,
       is_won,
       is_closed,
       is_edu_oss,
       is_ps_opp,
       is_sao,
       is_win_rate_calc,
       is_net_arr_pipeline_created,
       is_net_arr_closed_deal,
       is_new_logo_first_order,
       is_closed_won,
       is_web_portal_purchase,
       is_stage_1_plus,
       is_stage_3_plus,
       is_stage_4_plus,
       is_lost,
       is_open,
       is_active,
       is_risky,
       is_credit,
       is_renewal,
       is_refund,
       is_deleted,
       is_duplicate,
       is_excluded_from_pipeline_created,
       is_contract_reset,
       is_comp_new_logo_override,
       is_eligible_open_pipeline,
       is_eligible_asp_analysis,
       is_eligible_age_analysis,
       is_eligible_churn_contraction,
       is_booked_net_arr,
       is_downgrade,
       critical_deal_flag,

       lead_source,
       dr_partner_deal_type,
       partner_account,
       partner_account_name,
       partner_gitlab_program,
       calculated_partner_track,
       dr_status,
       distributor,
       dr_deal_id,
       dr_primary_registration,
       influence_partner,
       fulfillment_partner,
       fulfillment_partner_name,
       platform_partner,
       partner_track,
       resale_partner_track,
       is_public_sector_opp,
       is_registration_from_portal,
       calculated_discount,
       partner_discount,
       partner_discount_calc,
       comp_channel_neutral,
       count_crm_attribution_touchpoints,
       weighted_linear_iacv,
       count_campaigns,
       sa_tech_evaluation_close_status,
       sa_tech_evaluation_end_date,
       sa_tech_evaluation_start_date,
       cp_partner,
       cp_paper_process,
       cp_help,
       cp_review_notes,
       cp_champion,
       cp_close_plan,
       cp_decision_criteria,
       cp_decision_process,
       cp_economic_buyer,
       cp_identify_pain,
       cp_metrics,
       cp_risks,
       cp_value_driver,
       cp_why_do_anything_at_all,
       cp_why_gitlab,
       cp_why_now,
       cp_score,
       cp_use_cases,
       competitors,
       competitors_other_flag,
       competitors_gitlab_core_flag,
       competitors_none_flag,
       competitors_github_enterprise_flag,
       competitors_bitbucket_server_flag,
       competitors_unknown_flag,
       competitors_github_flag,
       competitors_gitlab_flag,
       competitors_jenkins_flag,
       competitors_azure_devops_flag,
       competitors_svn_flag,
       competitors_bitbucket_flag,
       competitors_atlassian_flag,
       competitors_perforce_flag,
       competitors_visual_studio_flag,
       competitors_azure_flag,
       competitors_amazon_code_commit_flag,
       competitors_circleci_flag,
       competitors_bamboo_flag,
       competitors_aws_flag,
       created_date,
       created_month,
       created_fiscal_quarter_date,
       created_fiscal_quarter_name,
       created_fiscal_year,
       sales_accepted_date,
       sales_accepted_month,
       sales_accepted_fiscal_quarter_date,
       sales_accepted_fiscal_quarter_name,
       sales_accepted_fiscal_year,
       close_date,
       close_month,
       close_fiscal_quarter_date,
       close_fiscal_quarter_name,
       close_fiscal_year,
       stage_0_pending_acceptance_date,
       stage_0_pending_acceptance_month,
       stage_0_pending_acceptance_fiscal_quarter_date,
       stage_0_pending_acceptance_fiscal_quarter_name,
       stage_0_pending_acceptance_fiscal_year,
       stage_1_discovery_date,
       stage_1_discovery_month,
       stage_1_discovery_fiscal_quarter_date,
       stage_1_discovery_fiscal_quarter_name,
       stage_1_discovery_fiscal_year,
       stage_2_scoping_date,
       stage_2_scoping_month,
       stage_2_scoping_fiscal_quarter_date,
       stage_2_scoping_fiscal_quarter_name,
       stage_2_scoping_fiscal_year,
       stage_3_technical_evaluation_date,
       stage_3_technical_evaluation_month,
       stage_3_technical_evaluation_fiscal_quarter_date,
       stage_3_technical_evaluation_fiscal_quarter_name,
       stage_3_technical_evaluation_fiscal_year,
       stage_4_proposal_date,
       stage_4_proposal_month,
       stage_4_proposal_fiscal_quarter_date,
       stage_4_proposal_fiscal_quarter_name,
       stage_4_proposal_fiscal_year,
       stage_5_negotiating_date,
       stage_5_negotiating_month,
       stage_5_negotiating_fiscal_quarter_date,
       stage_5_negotiating_fiscal_quarter_name,
       stage_5_negotiating_fiscal_year,
       stage_6_awaiting_signature_date_date,
       stage_6_awaiting_signature_date_month,
       stage_6_awaiting_signature_date_fiscal_quarter_date,
       stage_6_awaiting_signature_date_fiscal_quarter_name,
       stage_6_awaiting_signature_date_fiscal_year,
       stage_6_closed_won_date,
       stage_6_closed_won_month,
       stage_6_closed_won_fiscal_quarter_date,
       stage_6_closed_won_fiscal_quarter_name,
       stage_6_closed_won_fiscal_year,
       stage_6_closed_lost_date,
       stage_6_closed_lost_month,
       stage_6_closed_lost_fiscal_quarter_date,
       stage_6_closed_lost_fiscal_quarter_name,
       stage_6_closed_lost_fiscal_year,
       subscription_start_date,
       subscription_start_month,
       subscription_start_fiscal_quarter_date,
       subscription_start_fiscal_quarter_name,
       subscription_start_fiscal_year,
       subscription_end_date,
       subscription_end_month,
       subscription_end_fiscal_quarter_date,
       subscription_end_fiscal_quarter_name,
       subscription_end_fiscal_year,
       sales_qualified_date,
       sales_qualified_month,
       sales_qualified_fiscal_quarter_date,
       sales_qualified_fiscal_quarter_name,
       sales_qualified_fiscal_year,
       last_activity_date,
       last_activity_month,
       last_activity_fiscal_quarter_date,
       last_activity_fiscal_quarter_name,
       last_activity_fiscal_year,
       sales_last_activity_date,
       sales_last_activity_month,
       sales_last_activity_fiscal_quarter_date,
       sales_last_activity_fiscal_quarter_name,
       sales_last_activity_fiscal_year,
       technical_evaluation_date,
       technical_evaluation_month,
       technical_evaluation_fiscal_quarter_date,
       technical_evaluation_fiscal_quarter_name,
       technical_evaluation_fiscal_year,
       arr_created_date,
       arr_created_month,
       arr_created_fiscal_quarter_date,
       arr_created_fiscal_quarter_name,
       arr_created_fiscal_year,
       pipeline_created_date,
       pipeline_created_month,
       pipeline_created_fiscal_quarter_date,
       pipeline_created_fiscal_quarter_name,
       pipeline_created_fiscal_year,
       net_arr_created_date,
       net_arr_created_month,
       net_arr_created_fiscal_quarter_date,
       net_arr_created_fiscal_quarter_name,
       net_arr_created_fiscal_year,
       days_in_0_pending_acceptance,
       days_in_1_discovery,
       days_in_2_scoping,
       days_in_3_technical_evaluation,
       days_in_4_proposal,
       days_in_5_negotiating,
       days_in_sao,
       calculated_age_in_days,
       days_since_last_activity,
       arr_basis,

      -- NF 28072023 These fields will be updated
       arr_basis_for_clari        AS atr,
       won_arr_basis_for_clari    AS won_atr,

       CASE
        WHEN fpa_master_bookings_flag = 1
          THEN won_arr_basis_for_clari - arr_basis_for_clari
        ELSE 0
       END                         AS booked_churned_contraction_net_arr,
       
       ------------------------------------------
       iacv,
       net_iacv,
       segment_order_type_iacv_to_net_arr_ratio,
       calculated_from_ratio_net_arr,
       net_arr,
       created_and_won_same_quarter_net_arr,
       new_logo_count,
       amount,
       open_1plus_deal_count,
       open_3plus_deal_count,
       open_4plus_deal_count,
       booked_deal_count,
       churned_contraction_deal_count,
       open_1plus_net_arr,
       open_3plus_net_arr,
       open_4plus_net_arr,
       booked_net_arr,
       churned_contraction_net_arr,
       calculated_deal_count,
       booked_churned_contraction_deal_count,
       arr,
       recurring_amount,
       true_up_amount,
       proserv_amount,
       other_non_recurring_amount,
       renewal_amount,
       total_contract_value,
       days_in_stage,
       created_by,
       updated_by,
       model_created_date,
       model_updated_date,
       dbt_updated_at,
       dbt_created_at
 FROM {{ref('mart_crm_opportunity')}} 
 
 ), edm_extended AS ( 

    SELECT edm_opty.*,

    -----------------------------------------------------------------------------------------------------
    -----------------------------------------------------------------------------------------------------
    -- Opportunity User fields
    -- https://gitlab.my.salesforce.com/00N6100000ICcrD?setupid=OpportunityFields
    
    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    account_owner.adjusted_user_segment      AS account_owner_user_segment,
    account_owner.user_segment               AS account_owner_raw_user_segment,
    account_owner.user_geo                   AS account_owner_user_geo,
    account_owner.user_region                AS account_owner_user_region,
    account_owner.user_area                  AS account_owner_user_area,

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    account_owner.business_unit              AS account_owner_user_business_unit,
    account_owner.sub_business_unit          AS account_owner_user_sub_business_unit,
    account_owner.division                   AS account_owner_user_division,
    account_owner.asm                        AS account_owner_user_asm,
    account_owner.role_type                  AS account_owner_user_role_type,

    -- NF: 20230223 FY24 GTM fields, precalculated in the user object
    opportunity_owner.business_unit         AS opportunity_owner_user_business_unit,
    opportunity_owner.sub_business_unit     AS opportunity_owner_user_sub_business_unit,
    opportunity_owner.division              AS opportunity_owner_user_division,
    opportunity_owner.asm                   AS opportunity_owner_user_asm,
    opportunity_owner.role_type             AS opportunity_owner_user_role_type,

    opportunity_owner.adjusted_user_segment AS opportunity_owner_user_segment,
    opportunity_owner.user_segment          AS opportunity_owner_raw_user_segment,
    opportunity_owner.user_geo              AS opportunity_owner_user_geo,
    opportunity_owner.user_region           AS opportunity_owner_user_region,
    opportunity_owner.user_area             AS opportunity_owner_user_area,

    -- NF: adjusted to FY24 GTM structure
   CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.adjusted_user_segment
        -- TODO: Add hybrid reps logic
        ELSE opportunity_owner.adjusted_user_segment
    END                                                       AS report_opportunity_user_segment,

    -- NF: unadjusted version of segment used to create the FY24 GTM key
    CASE 
        WHEN account_owner.is_hybrid_flag = 1
          THEN account.parent_crm_account_sales_segment
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_segment
        ELSE opportunity_owner.user_segment
    END                                                       AS report_opportunity_raw_user_segment,

    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_geo
        ELSE opportunity_owner.user_geo
    END                                                       AS report_opportunity_user_geo,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_region
        ELSE opportunity_owner.user_region
    END                                                       AS report_opportunity_user_region,


/*

From Melia: 20230818
Hybrid reps work across segments (or areas, or whatever). like we have a guy who works on both MM and SMB accounts. 
he has a default user value of MM that makes all of his opps look like they are MM. instead of making them all MM, 
you are grabbing the segment off of the account instead so you'll show some as MM and some as SMB.

*/


   CASE
        WHEN account_owner.is_hybrid_flag = 1 
            THEN account.parent_crm_account_area
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.user_area
    -- NF: 20230818 VPs might temporary hold opportunities of territories without reps. As their AREA is ALL it needs to 
    -- be adjusted to the account AREA
        WHEN UPPER(opportunity_owner.user_area) = 'ALL'
           THEN account.parent_crm_account_area         
        ELSE opportunity_owner.user_area
    END                                                       AS report_opportunity_user_area,


    -- NF 20230214
    -- FY24 GTM calculated fields. These fields will be sourced from EDM eventually
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.business_unit
        ELSE opportunity_owner.business_unit
    END                                                       AS report_opportunity_user_business_unit,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.sub_business_unit
        ELSE opportunity_owner.sub_business_unit
    END                                                       AS report_opportunity_user_sub_business_unit,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.division
        ELSE opportunity_owner.division
    END                                                       AS report_opportunity_user_division,
    CASE 
        -- NF: see comment above on hybrid reps
        WHEN account_owner.is_hybrid_flag = 1 
            THEN account.parent_crm_account_area
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.asm
        -- NF: 20230818 VPs might temporary hold opportunities of territories without reps. As their AREA is ALL it needs to 
        -- be adjusted to the account AREA
        WHEN UPPER(opportunity_owner.user_area) = 'ALL'
           THEN account.parent_crm_account_area         
        ELSE opportunity_owner.asm
    END                                                       AS report_opportunity_user_asm,
    CASE 
        WHEN edm_opty.close_date < today.current_fiscal_year_date
          THEN account_owner.role_type
        ELSE opportunity_owner.role_type
    END                                                       AS report_opportunity_user_role_type,
  

    -- NF 20230210 These next two fields will be eventually sourced from the EDM
    CASE
      WHEN (edm_opty.sales_qualified_source_name = 'Partner Generated' OR edm_opty.sales_qualified_source_name = 'Partner Generated')
          THEN 'Partner Sourced'
      WHEN (edm_opty.sales_qualified_source_name != 'Partner Generated' AND edm_opty.sales_qualified_source_name != 'Partner Generated')
          AND NOT LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
          THEN 'Channel Co-Sell'
      WHEN (edm_opty.sales_qualified_source_name != 'Partner Generated' AND edm_opty.sales_qualified_source_name != 'Partner Generated')
          AND LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%','%amazon%')
          THEN 'Alliance Co-Sell'
      ELSE 'Direct'
    END AS partner_category,

    CASE
      WHEN LOWER(resale_account.account_name) LIKE ANY ('%google%','%gcp%')
        THEN 'GCP'
      WHEN LOWER(resale_account.account_name) LIKE ANY ('%amazon%')
        THEN 'AWS'
      WHEN LOWER(resale_account.account_name) IS NOT NULL
        THEN 'Channel'
      ELSE 'Direct'
    END                                               AS alliance_partner,

    ------------------------------------------------------------------------
    -- creating report_user_segment_geo_region_area_sqs_ot with adjusted segment
    LOWER(
      CONCAT(
        report_opportunity_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',
        sales_qualified_source_name,
        '-',
        deal_group
      )
    ) AS report_user_segment_geo_region_area_sqs_ot,

    -- creating report_user_segment_geo_region_area with adjusted segment
    LOWER(
      CONCAT(
        report_opportunity_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area
      )
    ) AS report_user_segment_geo_region_area,
    
    ------------------------------------------------------------------------
    -- FY24 keys
    -- These keys must leverage the unadjusted raw_segment field 
    
    LOWER(
      CONCAT(
        report_opportunity_user_business_unit,
        '-',
        report_opportunity_raw_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',
        sales_qualified_source_name,
        '-',
        deal_group
      )
    ) AS report_bu_user_segment_geo_region_area_sqs_ot,

   LOWER(
      CONCAT(
        report_opportunity_user_business_unit,
        '-',
        report_opportunity_user_sub_business_unit,
        '-',
        report_opportunity_user_division,
        '-',
        report_opportunity_user_asm,
        '-',
        report_opportunity_raw_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',
        edm_opty.sales_qualified_source_name,
        '-',
        edm_opty.deal_group
      )
    ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot,

    LOWER(
      CONCAT(
        report_opportunity_user_business_unit,
        '-',
        report_opportunity_user_sub_business_unit,
        '-',
        report_opportunity_user_division,
        '-',
        report_opportunity_user_asm,
        '-',
        report_opportunity_raw_user_segment,
        '-',
        report_opportunity_user_geo,
        '-',
        report_opportunity_user_region,
        '-',
        report_opportunity_user_area,
        '-',       
        edm_opty.sales_qualified_source_name,
        '-',
        edm_opty.deal_group,
        '-',
        report_opportunity_user_role_type,
        '-',
        partner_category,
        '-',
        alliance_partner
      )
    ) AS report_bu_subbu_division_asm_user_segment_geo_region_area_sqs_ot_rt_pc_ap,

    --- SOURCE fields
    -- NF: These should be moved eventually to the MART table
    opty_source.pushed_count,
    opty_source.intented_product_tier,
    opty_source.parent_opportunity,
    account.lam_dev_count

    --FROM prod.restricted_safe_common_mart_sales.mart_crm_opportunity
    FROM edm_opty
    CROSS JOIN today
    INNER JOIN sfdc_accounts_xf AS account
      ON account.account_id = edm_opty.dim_crm_account_id
    INNER JOIN sfdc_users_xf AS account_owner
      ON account_owner.user_id = account.owner_id
    INNER JOIN sfdc_users_xf AS opportunity_owner
      ON opportunity_owner.user_id = edm_opty.owner_id
    INNER JOIN sfdc_opportunity_source opty_source 
      ON opty_source.opportunity_id = edm_opty.dim_crm_opportunity_id
    LEFT JOIN sfdc_accounts_xf AS resale_account
      ON resale_account.account_id = edm_opty.fulfillment_partner

 )

 SELECT *
 FROM edm_extended
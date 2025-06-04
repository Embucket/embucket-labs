WITH sfdc_opportunity AS (

    SELECT * FROM {{ref('sfdc_opportunity')}}

), sfdc_opportunity_stage AS (

    SELECT * FROM {{ref('sfdc_opportunity_stage')}}

), sfdc_lead_source AS (

    SELECT * FROM {{ref('sfdc_lead_sources')}}

), sfdc_users_xf AS (

    SELECT * FROM {{ref('sfdc_users_xf')}}

), sfdc_record_type AS (

    SELECT *
    FROM {{ ref('sfdc_record_type') }}

), sfdc_account AS (

    SELECT * FROM {{ref('sfdc_account')}}

), date_details AS (

    SELECT
      *,
      DENSE_RANK() OVER (ORDER BY first_day_of_fiscal_quarter) AS quarter_number
    FROM {{ ref('date_details') }}
    ORDER BY 1 DESC

), sales_admin_hierarchy AS (

    SELECT
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.owner_id,
      'CRO'                                                           AS level_1
    FROM sfdc_opportunity
    -- sfdc Sales Admin user
    WHERE owner_id = '00561000000mpHTAAY'

), layered AS (

    SELECT
      -- keys
      sfdc_opportunity.account_id,
      sfdc_opportunity.opportunity_id,
      sfdc_opportunity.opportunity_name,
      sfdc_opportunity.owner_id,

      -- logistical information
      sfdc_opportunity.close_date,
      sfdc_opportunity.created_date,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.deployment_preference,
      sfdc_opportunity.generated_source,
      sfdc_opportunity.lead_source,
      sfdc_lead_source.lead_source_id                                                             AS lead_source_id,
      COALESCE(sfdc_lead_source.initial_source, 'Unknown')                                        AS lead_source_name,
      COALESCE(sfdc_lead_source.initial_source_type, 'Unknown')                                   AS lead_source_type,
      sfdc_opportunity.merged_opportunity_id,
      sfdc_opportunity.net_new_source_categories,
      sfdc_opportunity.crm_business_dev_rep_id,
      sfdc_opportunity.opportunity_owner                                                          AS opportunity_owner,
      sfdc_opportunity.opportunity_owner_department                                               AS opportunity_owner_department,
      sfdc_opportunity.opportunity_owner_manager                                                  AS opportunity_owner_manager,
      opportunity_owner.role_name                                                                 AS opportunity_owner_role,
      opportunity_owner.title                                                                     AS opportunity_owner_title,
      sfdc_opportunity.crm_sales_dev_rep_id,
      sfdc_opportunity.opportunity_development_representative,
      sfdc_opportunity.opportunity_term,
      sfdc_opportunity.primary_campaign_source_id                                                 AS primary_campaign_source_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_path,
      sfdc_opportunity.sales_qualified_date,
      sfdc_opportunity.subscription_type,
      sfdc_opportunity.sdr_pipeline_contribution,
      sfdc_opportunity.source_buckets,
      sfdc_opportunity.stage_name,
      sfdc_opportunity_stage.is_active                                                             AS stage_is_active,
      sfdc_opportunity_stage.is_closed                                                             AS stage_is_closed,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.deal_path,
      sfdc_opportunity.opportunity_category,

      -- opportunity information
      sfdc_opportunity.acv,
      sfdc_opportunity.amount,
      sfdc_opportunity.is_closed_deals,
      sfdc_opportunity.competitors,
      sfdc_opportunity.critical_deal_flag,
      sfdc_opportunity.deal_size,
      sfdc_opportunity.forecast_category_name,
      sfdc_opportunity.forecasted_iacv,
      sfdc_opportunity.iacv_created_date,
      sfdc_opportunity.incremental_acv,
      sfdc_opportunity.invoice_number,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      CASE
        WHEN (sfdc_opportunity.days_in_stage > 30
          OR sfdc_opportunity.incremental_acv > 100000
          OR sfdc_opportunity.pushed_count > 0)
            THEN TRUE
        ELSE FALSE
      END                                                                                         AS is_risky,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity_stage.is_won                                                               AS is_won,
      sfdc_opportunity.net_incremental_acv,
      sfdc_opportunity.probability,
      sfdc_opportunity.professional_services_value,
      sfdc_opportunity.pushed_count,
      sfdc_opportunity.reason_for_loss,
      sfdc_opportunity.reason_for_loss_details,
      sfdc_opportunity.downgrade_reason,
      sfdc_opportunity.refund_iacv,
      sfdc_opportunity.downgrade_iacv,
      sfdc_opportunity.renewal_acv,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.sales_qualified_source,
      sfdc_opportunity.solutions_to_be_replaced,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.upside_swing_deal_iacv,
      sfdc_opportunity.incremental_acv * (probability /100)                                       AS weighted_iacv,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.partner_initiated_opportunity,
      sfdc_opportunity.user_segment,
      sfdc_opportunity.subscription_start_date,
      sfdc_opportunity.subscription_end_date,
      sfdc_opportunity.true_up_value,
      sfdc_opportunity.order_type_current,
      sfdc_opportunity.order_type_stamped,
      sfdc_opportunity.net_arr,
      sfdc_opportunity.recurring_amount,
      sfdc_opportunity.true_up_amount,
      sfdc_opportunity.proserv_amount,
      sfdc_opportunity.other_non_recurring_amount,
      sfdc_opportunity.arr_basis,
      sfdc_opportunity.arr,
      sfdc_opportunity.opportunity_health,
      sfdc_opportunity.risk_type,
      sfdc_opportunity.risk_reasons,
      sfdc_opportunity.tam_notes,
      sfdc_opportunity.primary_solution_architect,
      sfdc_opportunity.product_details,
      sfdc_opportunity.product_category,
      sfdc_opportunity.products_purchased,

      -- days and dates per stage
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.stage_0_pending_acceptance_date,
      sfdc_opportunity.stage_1_discovery_date,
      sfdc_opportunity.stage_2_scoping_date,
      sfdc_opportunity.stage_3_technical_evaluation_date,
      sfdc_opportunity.stage_4_proposal_date,
      sfdc_opportunity.stage_5_negotiating_date,
      sfdc_opportunity.stage_6_awaiting_signature_date,
      sfdc_opportunity.stage_6_closed_won_date,
      sfdc_opportunity.stage_6_closed_lost_date,

      -- helper flag, tracks won deals & renewals + not jihu
      sfdc_opportunity.fpa_master_bookings_flag,

      -- command plan fields
      sfdc_opportunity.cp_champion,
      sfdc_opportunity.cp_close_plan,
      sfdc_opportunity.cp_decision_criteria,
      sfdc_opportunity.cp_decision_process,
      sfdc_opportunity.cp_economic_buyer,
      sfdc_opportunity.cp_identify_pain,
      sfdc_opportunity.cp_metrics,
      sfdc_opportunity.cp_risks,
      sfdc_opportunity.cp_use_cases,
      sfdc_opportunity.cp_value_driver,
      sfdc_opportunity.cp_why_do_anything_at_all,
      sfdc_opportunity.cp_why_gitlab,
      sfdc_opportunity.cp_why_now,
      sfdc_opportunity.cp_score,

      -- User Segment Hierarchy fields
      sfdc_opportunity.user_segment_stamped,
      sfdc_opportunity.user_geo_stamped,
      sfdc_opportunity.user_region_stamped,
      sfdc_opportunity.user_area_stamped,

      -- sales segment refactor
      sfdc_opportunity.division_sales_segment_stamped,
      sfdc_account.account_sales_segment                           AS parent_crm_account_sales_segment,
      sfdc_account.division_sales_segment,
      sfdc_account.ultimate_parent_sales_segment,
      sfdc_account.is_jihu_account,
      sfdc_account.gitlab_partner_program,

      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/5709
      sfdc_opportunity.sales_segment                                                                    AS sales_segment,
      sfdc_opportunity.parent_segment                                                                   AS parent_segment,

      -- ************************************
      -- channel reporting
      -- issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072

      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,
      sfdc_opportunity.partner_account,
      sfdc_opportunity.dr_status,
      sfdc_opportunity.distributor,
      sfdc_opportunity.influence_partner,
      sfdc_opportunity.fulfillment_partner,
      sfdc_opportunity.platform_partner,
      sfdc_opportunity.partner_track,
      sfdc_opportunity.is_public_sector_opp,
      sfdc_opportunity.is_registration_from_portal,
      sfdc_opportunity.calculated_discount,
      sfdc_opportunity.partner_discount,
      sfdc_opportunity.partner_discount_calc,
      sfdc_opportunity.comp_channel_neutral,

      -- account owner hierarchies levels
      account_owner.sales_team_level_4                                                                    AS account_owner_team_level_4,
      account_owner.sales_team_vp_level                                                                   AS account_owner_team_vp_level,
      account_owner.sales_team_rd_level                                                                   AS account_owner_team_rd_level,
      account_owner.sales_team_asm_level                                                                  AS account_owner_team_asm_level,
      account_owner.sales_min_hierarchy_level                                                             AS account_owner_min_team_level,
      account_owner.sales_region                                                                          AS account_owner_sales_region,


      -- reporting helper flags
      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('0-Pending Acceptance','0-Qualifying','Developing', '1-Discovery', '2-Developing', '2-Scoping')
            THEN 'Pipeline'
        WHEN sfdc_opportunity.stage_name
          IN ('3-Technical Evaluation', '4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN '3+ Pipeline'
        WHEN sfdc_opportunity.stage_name
          IN ('8-Closed Lost', 'Closed Lost')
            THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
            THEN 'Closed Won'
        ELSE 'Other'
      END                                                                                                AS stage_name_3plus,

      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('0-Pending Acceptance','0-Qualifying','Developing','1-Discovery', '2-Developing', '2-Scoping', '3-Technical Evaluation')
            THEN 'Pipeline'
        WHEN sfdc_opportunity.stage_name
          IN ('4-Proposal', '5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN '4+ Pipeline'
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', 'Closed Lost')
            THEN 'Lost'
        WHEN sfdc_opportunity.stage_name IN ('Closed Won')
            THEN 'Closed Won'
        ELSE 'Other'
      END                                                                                                AS stage_name_4plus,

      CASE
        WHEN sfdc_opportunity.stage_name
          IN ('3-Technical Evaluation', '4-Proposal', 'Closed Won','5-Negotiating', '6-Awaiting Signature', '7-Closing')
            THEN 1
        ELSE 0
      END                                                                                               AS is_stage_3_plus,

      CASE
        WHEN sfdc_opportunity.stage_name = '8-Closed Lost'
          THEN 1 ELSE 0
      END                                                                                               AS is_lost,
      CASE
        WHEN sfdc_opportunity.stage_name IN ('8-Closed Lost', '9-Unqualified', 'Closed Won', '10-Duplicate') 
            THEN 0
        ELSE 1
      END                                                                                               AS is_open,

      CASE
        WHEN is_open = 0
          THEN 1
        ELSE 0
      END                                                                                               AS is_closed,

      CASE
        WHEN sfdc_opportunity_stage.is_won = 1 THEN '1.Won'
        WHEN is_lost = 1 THEN '2.Lost'
        WHEN is_open = 1 THEN '0. Open'
        ELSE 'N/A'
      END                                                                                               AS stage_category,

      CASE
        WHEN LOWER(sfdc_opportunity.subscription_type) like '%renewal%'
          THEN 1
        ELSE 0
      END                                                                                               AS is_renewal,

      -- date fields helpers
      close_date_detail.fiscal_quarter_name_fy                             AS close_fiscal_quarter_name,
      close_date_detail.first_day_of_fiscal_quarter                        AS close_fiscal_quarter_date,
      close_date_detail.fiscal_year                                        AS close_fiscal_year,
      close_date_detail.first_day_of_month                                 AS close_date_month,

      created_date_detail.fiscal_quarter_name_fy                           AS created_fiscal_quarter_name,
      created_date_detail.first_day_of_fiscal_quarter                      AS created_fiscal_quarter_date,
      created_date_detail.fiscal_year                                      AS created_fiscal_year,
      created_date_detail.first_day_of_month                               AS created_date_month,

      start_date.fiscal_quarter_name_fy                                    AS subscription_start_date_fiscal_quarter_name,
      start_date.first_day_of_fiscal_quarter                               AS subscription_start_date_fiscal_quarter_date,
      start_date.fiscal_year                                               AS subscription_start_date_fiscal_year,
      start_date.first_day_of_month                                        AS subscription_start_date_month,

      sales_accepted_date.fiscal_quarter_name_fy                           AS sales_accepted_fiscal_quarter_name,
      sales_accepted_date.first_day_of_fiscal_quarter                      AS sales_accepted_fiscal_quarter_date,
      sales_accepted_date.fiscal_year                                      AS sales_accepted_fiscal_year,
      sales_accepted_date.first_day_of_month                               AS sales_accepted_date_month,

      sales_qualified_date.fiscal_quarter_name_fy                          AS sales_qualified_fiscal_quarter_name,
      sales_qualified_date.first_day_of_fiscal_quarter                     AS sales_qualified_fiscal_quarter_date,
      sales_qualified_date.fiscal_year                                     AS sales_qualified_fiscal_year,
      sales_qualified_date.first_day_of_month                              AS sales_qualified_date_month,

      iacv_created_date.fiscal_quarter_name_fy                             AS iacv_created_fiscal_quarter_name,
      iacv_created_date.first_day_of_fiscal_quarter                        AS iacv_created_fiscal_quarter_date,
      iacv_created_date.fiscal_year                                        AS iacv_created_fiscal_year,
      iacv_created_date.first_day_of_month                                 AS iacv_created_date_month,

      -- metadata
      sfdc_opportunity._last_dbt_run,
      sfdc_record_type.business_process_id,
      sfdc_opportunity.days_since_last_activity,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.last_activity_date,
      sfdc_record_type.record_type_description,
      sfdc_opportunity.record_type_id,
      sfdc_record_type.record_type_label,
      sfdc_record_type.record_type_modifying_object_type,
      sfdc_record_type.record_type_name,
      MD5((DATE_TRUNC('month', sfdc_opportunity.close_date)::DATE)||UPPER(opportunity_owner.team))      AS region_quota_id,
      MD5((DATE_TRUNC('month', sfdc_opportunity.close_date)::DATE)||UPPER(opportunity_owner.name))      AS sales_quota_id

    FROM sfdc_opportunity
    INNER JOIN sfdc_opportunity_stage
      ON sfdc_opportunity.stage_name = sfdc_opportunity_stage.primary_label
    INNER JOIN date_details close_date_detail
      ON close_date_detail.date_actual = sfdc_opportunity.close_date::DATE
    INNER JOIN date_details created_date_detail
      ON created_date_detail.date_actual = sfdc_opportunity.created_date::DATE
    LEFT JOIN sfdc_lead_source
      ON sfdc_opportunity.lead_source = sfdc_lead_source.initial_source
    LEFT JOIN sfdc_users_xf opportunity_owner
      ON sfdc_opportunity.owner_id = opportunity_owner.user_id
    LEFT JOIN sfdc_record_type
      ON sfdc_opportunity.record_type_id = sfdc_record_type.record_type_id
    LEFT JOIN sfdc_account
      ON sfdc_account.account_id = sfdc_opportunity.account_id
    LEFT JOIN date_details sales_accepted_date
      ON sfdc_opportunity.sales_accepted_date::DATE = sales_accepted_date.date_actual
    LEFT JOIN date_details start_date
      ON sfdc_opportunity.subscription_start_date::DATE = start_date.date_actual
    LEFT JOIN date_details sales_qualified_date
      ON sfdc_opportunity.sales_qualified_date::DATE = sales_qualified_date.date_actual
    LEFT JOIN date_details iacv_created_date
      ON iacv_created_date.date_actual = sfdc_opportunity.iacv_created_date::DATE
    LEFT JOIN sfdc_users_xf account_owner
      ON account_owner.user_id = sfdc_account.owner_id
    LEFT JOIN sales_admin_hierarchy
      ON sfdc_opportunity.opportunity_id = sales_admin_hierarchy.opportunity_id
)

SELECT *
FROM layered

/*

  ATTENTION: When a field is added to this live model, add it to the SFDC_OPPORTUNITY_SNAPSHOTS_SOURCE model to keep the live and snapshot models in alignment.

*/

WITH source AS (

    SELECT
      opportunity.*,
      CASE
        WHEN stagename = '0-Pending Acceptance'     THEN x0_pending_acceptance_date__c
        WHEN stagename = '1-Discovery'              THEN x1_discovery_date__c
        WHEN stagename = '2-Scoping'                THEN x2_scoping_date__c
        WHEN stagename = '3-Technical Evaluation'   THEN x3_technical_evaluation_date__c
        WHEN stagename = '4-Proposal'               THEN x4_proposal_date__c
        WHEN stagename = '5-Negotiating'            THEN x5_negotiating_date__c
        WHEN stagename = '6-Awaiting Signature'     THEN x6_awaiting_signature_date__c
      END                                                                                   AS calculation_days_in_stage_date,
      DATEDIFF(days,calculation_days_in_stage_date::DATE,CURRENT_DATE::DATE) + 1            AS days_in_stage
    FROM {{ source('salesforce_sandbox', 'opportunity') }}  AS opportunity

), renamed AS (

      SELECT
        -- keys
        accountid                                       AS account_id,
        id                                              AS opportunity_id,
        name                                            AS opportunity_name,
        ownerid                                         AS owner_id,

        -- logistical information
        isclosed                                        AS is_closed,
        iswon                                           AS is_won,
        closedate                                       AS close_date,
        createddate                                     AS created_date,
        days_in_stage                                   AS days_in_stage,
        deployment_preference__c                        AS deployment_preference,
        sql_source__c                                   AS generated_source,
        leadsource                                      AS lead_source,
        merged_opportunity__c                           AS merged_opportunity_id,
        duplicate_opportunity__c                        AS duplicate_opportunity_id,
        account_owner__c                                AS account_owner,
        opportunity_owner__c                            AS opportunity_owner,
        manager_current__c                              AS opportunity_owner_manager,
        sales_market__c                                 AS opportunity_owner_department,
        SDR_LU__c                                       AS opportunity_crm_sales_dev_rep_id,
        business_development_representative__c          AS crm_business_dev_rep_id,
        NULL                                            AS crm_business_dev_rep_id_lookup,
        NULL                                            AS opportunity_development_representative,

        sales_accepted_date__c                          AS sales_accepted_date,
        engagement_type__c                              AS sales_path,
        sales_qualified_date__c                         AS sales_qualified_date,
        iqm_submitted_by_role__c                        AS iqm_submitted_by_role,

        type                                            AS subscription_type,
        {{  sfdc_source_buckets('leadsource') }}
        stagename                                       AS stage_name,
        {{ deal_path_cleaning('deal_path__c') }}        AS deal_path,

        -- opportunity information
        acv_2__c                                        AS acv,
        amount                                          AS amount,
        IFF(acv_2__c >= 0, 1, 0)                        AS is_closed_deals, -- so that you can exclude closed deals that had negative impact
        competitors__c                                  AS competitors,
        critical_deal_flag__c                           AS critical_deal_flag,
        {{sfdc_deal_size('arr_net__c', 'deal_size')}},
        forecastcategoryname                            AS forecast_category_name,
        incremental_acv_2__c                            AS forecasted_iacv,
        iacv_created_date__c                            AS iacv_created_date,
        incremental_acv__c                              AS incremental_acv,
        invoice_number__c                               AS invoice_number,
        is_refund_opportunity__c                        AS is_refund,
        is_downgrade_opportunity__c                     AS is_downgrade,
        swing_deal__c                                   AS is_swing_deal,
        is_edu_oss_opportunity__c                       AS is_edu_oss,
        is_ps_opportunity__c                            AS is_ps_opp,
        net_iacv__c                                     AS net_incremental_acv,
        campaignid                                      AS primary_campaign_source_id,
        probability                                     AS probability,
        professional_services_value__c                  AS professional_services_value,
        push_counter__c                                 AS pushed_count,
        reason_for_lost__c                              AS reason_for_loss,
        reason_for_lost_details__c                      AS reason_for_loss_details,
        refund_iacv__c                                  AS refund_iacv,
        downgrade_iacv__c                               AS downgrade_iacv,
        renewal_acv__c                                  AS renewal_acv,
        renewal_amount__c                               AS renewal_amount,
        {{ sales_qualified_source_cleaning('sql_source__c') }}
                                                        AS sales_qualified_source,
        {{ sales_qualified_source_grouped('sales_qualified_source') }}  AS sales_qualified_source_grouped,
        {{ sqs_bucket_engagement('sales_qualified_source') }}           AS sqs_bucket_engagement,
        sdr_pipeline_contribution__c                    AS sdr_pipeline_contribution,
        solutions_to_be_replaced__c                     AS solutions_to_be_replaced,
        x3_technical_evaluation_date__c
                                                        AS technical_evaluation_date,
        amount                                          AS total_contract_value,
        recurring_amount__c                             AS recurring_amount,
        true_up_amount__c                               AS true_up_amount,
        proserv_amount__c                               AS proserv_amount,
        other_non_recurring_amount__c                   AS other_non_recurring_amount,
        upside_swing_deal_iacv__c                       AS upside_swing_deal_iacv,
        web_portal_purchase__c                          AS is_web_portal_purchase,
        opportunity_term_new__c                         AS opportunity_term,
        NULL                                            AS partner_initiated_opportunity,
        user_segment_o__c                               AS user_segment,
        start_date__c::DATE                             AS subscription_start_date,
        end_date__c::DATE                               AS subscription_end_date,
        NULL                                            AS true_up_value,
        order_type_live__c                              AS order_type_current,
        order_type_test__c                              AS order_type_stamped,
        CASE
          WHEN order_type_stamped = '1. New - First Order'
            THEN '1) New - First Order'
          WHEN order_type_stamped IN ('2. New - Connected', '3. Growth', '4. Contraction', '5. Churn - Partial', '6. Churn - Final')
            THEN '2) Growth (Growth / New - Connected / Churn / Contraction)'
          WHEN order_type_stamped IN ('7. PS / Other')
            THEN '3) Consumption / PS / Other'
          ELSE 'Missing order_type_name_grouped'
        END                                             AS order_type_grouped,
        {{ growth_type('order_type_test__c', 'arr_basis__c') }}
                                                        AS growth_type,
        arr_net__c                                      AS net_arr,
        arr_basis__c                                    AS arr_basis,
        arr__c                                          AS arr,
        days_in_sao__c                                  AS days_in_sao,
        new_logo_count__c                               AS new_logo_count,
        {{ sales_hierarchy_sales_segment_cleaning('user_segment_o__c') }}
                                                        AS user_segment_stamped,
        CASE
          WHEN user_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
          ELSE user_segment_stamped
        END                                             AS user_segment_stamped_grouped,
        stamped_user_geo__c                             AS user_geo_stamped,
        stamped_user_region__c                          AS user_region_stamped,
        stamped_user_area__c                            AS user_area_stamped,
        {{ sales_segment_region_grouped('user_segment_stamped', 'user_geo_stamped', 'user_region_stamped') }}
                                                        AS user_segment_region_stamped_grouped,
        CONCAT(user_segment_stamped,
               '-',
               user_geo_stamped,
               '-',
               user_region_stamped,
               '-',
               user_area_stamped
              )                                         AS user_segment_geo_region_area_stamped,
        stamped_opp_owner_user_role_type__c             AS crm_opp_owner_user_role_type_stamped,
        stamped_opp_owner_user_business_unit__c         AS user_business_unit_stamped,
        stamped_opportunity_owner__c                    AS crm_opp_owner_stamped_name,
        stamped_account_owner__c                        AS crm_account_owner_stamped_name,
        sao_user_segment__c                             AS sao_crm_opp_owner_sales_segment_stamped,
        NULL                                            AS sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
        CASE
          WHEN sao_crm_opp_owner_sales_segment_stamped IN ('Large', 'PubSec') THEN 'Large'
          ELSE sao_crm_opp_owner_sales_segment_stamped
        END                                             AS sao_crm_opp_owner_sales_segment_stamped_grouped,
        sao_user_geo__c                                 AS sao_crm_opp_owner_geo_stamped,
        sao_user_region__c                              AS sao_crm_opp_owner_region_stamped,
        sao_user_area__c                                AS sao_crm_opp_owner_area_stamped,
        {{ sales_segment_region_grouped('sao_crm_opp_owner_sales_segment_stamped', 'sao_crm_opp_owner_geo_stamped', 'sao_crm_opp_owner_region_stamped') }}
                                                        AS sao_crm_opp_owner_segment_region_stamped_grouped,
        opportunity_category__c                         AS opportunity_category,
        opportunity_health__c                           AS opportunity_health,
        NULL                                            AS risk_type,
        NULL                                            AS risk_reasons,
        tam_notes__c                                    AS tam_notes,
        solution_architect__c                           AS primary_solution_architect,
        product_details__c                              AS product_details,
        product_category__c                             AS product_category,
        products_purchased__c                           AS products_purchased,
        CASE
          WHEN web_portal_purchase__c THEN 'Web Direct'
          WHEN arr_net__c < 5000 THEN '<5K'
          WHEN arr_net__c < 25000 THEN '5-25K'
          WHEN arr_net__c < 100000 THEN '25-100K'
          WHEN arr_net__c < 250000 THEN '100-250K'
          WHEN arr_net__c > 250000 THEN '250K+'
          ELSE 'Missing opportunity_deal_size'
        END opportunity_deal_size,
        payment_schedule__c                             AS payment_schedule,
        comp_new_logo_override__c                       AS comp_new_logo_override,
        is_pipeline_created_eligible_flag__c            AS is_pipeline_created_eligible,
        next_steps__c                                   AS next_steps,
        auto_renewal_status__c                          AS auto_renewal_status,
        qsr_notes__c                                    AS qsr_notes,
        qsr_status__c                                   AS qsr_status,
        manager_forecast_confidence__c                  AS manager_confidence,
        renewal_risk_forecast__c                        AS renewal_risk_category,
        renewal_swing_arr__c                            AS renewal_swing_arr,
        isr__c                                          AS renewal_manager,
        renewal_forecast_category__c                    AS renewal_forecast_health,



      -- ************************************
      -- sales segmentation deprecated fields - 2020-09-03
      -- left temporary for the sake of MVC and avoid breaking SiSense existing charts
        COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, 'Unknown' )
                                                        AS sales_segment,
        {{ sales_segment_cleaning('ultimate_parent_sales_segment_emp_o__c') }}
                                                        AS parent_segment,
      -- ************************************

        -- dates in stage fields
        days_in_0_pending_acceptance__c                 AS days_in_0_pending_acceptance,
        days_in_1_discovery__c                          AS days_in_1_discovery,
        days_in_2_scoping__c                            AS days_in_2_scoping,
        days_in_3_technical_evaluation__c               AS days_in_3_technical_evaluation,
        days_in_4_proposal__c                           AS days_in_4_proposal,
        days_in_5_negotiating__c                        AS days_in_5_negotiating,

        x0_pending_acceptance_date__c                   AS stage_0_pending_acceptance_date,
        x1_discovery_date__c                            AS stage_1_discovery_date,
        x2_scoping_date__c                              AS stage_2_scoping_date,
        x3_technical_evaluation_date__c                 AS stage_3_technical_evaluation_date,
        x4_proposal_date__c                             AS stage_4_proposal_date,
        x5_negotiating_date__c                          AS stage_5_negotiating_date,
        x6_awaiting_signature_date__c                   AS stage_6_awaiting_signature_date,
        x6_closed_won_date__c                           AS stage_6_closed_won_date,
        x7_closed_lost_date__c                          AS stage_6_closed_lost_date,

        -- sales segment fields
        COALESCE({{ sales_segment_cleaning('sales_segmentation_employees_o__c') }}, 'Unknown' )
                                                        AS division_sales_segment_stamped,
        -- channel reporting
        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6072
        dr_partner_deal_type__c                         AS dr_partner_deal_type,
        dr_partner_engagement__c                        AS dr_partner_engagement,
        vartopiadrs__dr_deal_reg_id__c                  AS dr_deal_id,
        vartopiadrs__primary_registration__c            AS dr_primary_registration,
        {{ channel_type('sqs_bucket_engagement', 'order_type_stamped') }}
                                                        AS channel_type,
        impartnerprm__partneraccount__c                 AS partner_account,
        vartopiadrs__dr_status1__c                      AS dr_status,
        distributor__c                                  AS distributor,
        influence_partner__c                            AS influence_partner,
        focus_partner__c                                AS is_focus_partner,
        fulfillment_partner__c                          AS fulfillment_partner,
        platform_partner__c                             AS platform_partner,
        partner_track__c                                AS partner_track,
        resale_partner_track__c                         AS resale_partner_track,
        public_sector_opp__c::BOOLEAN                   AS is_public_sector_opp,
        registration_from_portal__c::BOOLEAN            AS is_registration_from_portal,
        calculated_discount__c                          AS calculated_discount,
        partner_discount__c                             AS partner_discount,
        partner_discount_calc__c                        AS partner_discount_calc,
        NULL                                            AS comp_channel_neutral,

        -- command plan fields
        fm_champion__c                                  AS cp_champion,
        fm_close_plan__c                                AS cp_close_plan,
        fm_decision_criteria__c                         AS cp_decision_criteria,
        fm_decision_process__c                          AS cp_decision_process,
        fm_economic_buyer__c                            AS cp_economic_buyer,
        fm_help__c                                      AS cp_help,
        fm_identify_pain__c                             AS cp_identify_pain,
        fm_metrics__c                                   AS cp_metrics,
        fm_partner__c                                   AS cp_partner,
        fm_paper_process__c                             AS cp_paper_process,
        fm_review_notes__c                              AS cp_review_notes,
        fm_risks__c                                     AS cp_risks,
        fm_use_cases__c                                 AS cp_use_cases,
        fm_value_driver__c                              AS cp_value_driver,
        fm_why_do_anything_at_all__c                    AS cp_why_do_anything_at_all,
        fm_why_gitlab__c                                AS cp_why_gitlab,
        fm_why_now__c                                   AS cp_why_now,
        fm_score__c                                     AS cp_score,

        -- original issue: https://gitlab.com/gitlab-data/analytics/-/issues/6577
        sa_validated_tech_evaluation_close_statu__c     AS sa_tech_evaluation_close_status,
        sa_validated_tech_evaluation_end_date__c        AS sa_tech_evaluation_end_date,
        sa_validated_tech_evaluation_start_date__c      AS sa_tech_evaluation_start_date,

        -- flag to identify eligible booking deals, excluding jihu - issue: https://gitlab.com/gitlab-com/sales-team/field-operations/systems/-/issues/1805
        fp_a_master_bookings_flag__c::BOOLEAN           AS fpa_master_bookings_flag,
        downgrade_reason__c                             AS downgrade_reason,
        ssp_id__c                                       AS ssp_id,
        gaclientid__c                                   AS ga_client_id,

        -- original issue: https://gitlab.com/gitlab-com/sales-team/field-operations/customer-success-operations/-/issues/2464
        downgrade_details__c                            AS downgrade_details,
        won_arr_basis_for_clari__c                      AS won_arr_basis_for_clari,
        arr_basis_for_clari__c                          AS arr_basis_for_clari,
        forecasted_churn_for_clari__c                   AS forecasted_churn_for_clari,
        override_arr_basis_clari__c                     AS override_arr_basis_clari,

        -- ps fields - issue: https://gitlab.com/gitlab-com/sales-team/field-operations/customer-success-operations/-/issues/2723
        intended_product_tier__c                        AS intended_product_tier,
        parent_opportunity__c                           AS parent_opportunity_id,

        -- metadata
        convert_timezone('America/Los_Angeles',convert_timezone('UTC',
                 CURRENT_TIMESTAMP()))                  AS _last_dbt_run,
        DATEDIFF(days, lastactivitydate::date,
                         CURRENT_DATE)                  AS days_since_last_activity,
        isdeleted                                       AS is_deleted,
        lastactivitydate                                AS last_activity_date,
        sales_last_activity_date__c                     AS sales_last_activity_date,
        recordtypeid                                    AS record_type_id

      FROM source
  )

SELECT *
FROM renamed

{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('rpt_lead_to_revenue','rpt_lead_to_revenue')
  ]) 
}}

, cohort_base AS (

    SELECT DISTINCT
        dim_crm_person_id,
        sfdc_record_id,
        email_hash,
        sfdc_record_type,
        person_first_country,
        source_buckets,
        status AS person_status,
        lead_source,
        account_demographics_sales_segment,
        account_demographics_region,
        account_demographics_geo,
        account_demographics_area,
        account_demographics_upa_country,
        account_demographics_territory,
        person_order_type,
        is_mql,
        dim_crm_opportunity_id,
        opp_order_type,
        sales_qualified_source_name,
        is_won,
        valid_deal_count,
        is_sao,
        net_arr,
        is_net_arr_closed_deal,
        is_net_arr_pipeline_created,
        is_eligible_age_analysis,
        report_segment,
        report_geo,
        report_region,
        report_area,
        report_role_name,
        report_role_level_1,
        report_role_level_2,
        report_role_level_3,
        report_role_level_4,
        report_role_level_5,
        opp_account_demographics_sales_segment,
        opp_account_demographics_region,
        opp_account_demographics_geo,
        opp_account_demographics_territory,
        opp_account_demographics_area,
        true_inquiry_date_pt,
        mql_date_latest_pt,
        opp_created_date,
        sales_accepted_date,
        close_date
    FROM rpt_lead_to_revenue
    WHERE dim_crm_person_id IS NOT NULL
        OR dim_crm_opportunity_id IS NOT NULL

), final AS (

    SELECT
        dim_crm_person_id,
        sfdc_record_id,
        email_hash,
        sfdc_record_type,
        person_first_country,
        source_buckets,
        person_status,
        lead_source,
        account_demographics_sales_segment,
        account_demographics_region,
        account_demographics_geo,
        account_demographics_area,
        account_demographics_upa_country,
        account_demographics_territory,
        person_order_type,
        is_mql,
        dim_crm_opportunity_id,
        opp_order_type,
        sales_qualified_source_name,
        is_won,
        valid_deal_count,
        is_sao,
        net_arr,
        is_net_arr_closed_deal,
        is_net_arr_pipeline_created,
        is_eligible_age_analysis,
        report_segment,
        report_geo,
        report_region,
        report_area,
        report_role_name,
        report_role_level_1,
        report_role_level_2,
        report_role_level_3,
        report_role_level_4,
        report_role_level_5,
        opp_account_demographics_sales_segment,
        opp_account_demographics_region,
        opp_account_demographics_geo,
        opp_account_demographics_territory,
        opp_account_demographics_area,
        true_inquiry_date_pt,
        mql_date_latest_pt,
        opp_created_date,
        sales_accepted_date,
        close_date,
        CASE
            WHEN true_inquiry_date_pt IS NOT NULL AND mql_date_latest_pt IS NOT NULL
                THEN SUM(mql_date_latest_pt - true_inquiry_date_pt)
            ELSE NULL
        END AS inquiry_to_mql_days,
        CASE
            WHEN mql_date_latest_pt IS NOT NULL AND opp_created_date IS NOT NULL
                THEN SUM(opp_created_date - mql_date_latest_pt)
            ELSE NULL
        END AS mql_to_opp_days,
        CASE
            WHEN mql_date_latest_pt IS NOT NULL AND sales_accepted_date IS NOT NULL
                THEN SUM(sales_accepted_date - mql_date_latest_pt)
            ELSE NULL
        END AS mql_to_sao_days,
        CASE
            WHEN opp_created_date IS NOT NULL AND sales_accepted_date IS NOT NULL
                THEN SUM(sales_accepted_date - opp_created_date)
            ELSE NULL
        END AS opp_to_sao_days,
        CASE
            WHEN sales_accepted_date IS NOT NULL AND close_date IS NOT NULL
                THEN SUM(close_date - sales_accepted_date)
            ELSE NULL
        END AS sao_to_close_days        
    FROM cohort_base
    {{dbt_utils.group_by(n=46)}}

) 

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2024-06-20",
    updated_date="2024-10-07",
) }}
{{ simple_cte([
    ('dim_crm_account','dim_crm_account'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('mart_crm_person','mart_crm_person'),
    ('dim_crm_user','dim_crm_user')
    
]) }}

, upa_base AS (
  
    SELECT 
      dim_parent_crm_account_id,
      dim_crm_account_id
    FROM dim_crm_account
  
  ), first_order_opps AS (

    SELECT *
    FROM mart_crm_opportunity
    WHERE is_new_logo_first_order = true
    
  ), accounts_with_first_order_opps AS (

    SELECT
      upa_base.dim_parent_crm_account_id,
      upa_base.dim_crm_account_id,
      first_order_opps.dim_crm_opportunity_id,
      False AS is_first_order_available
    FROM upa_base 
    LEFT JOIN first_order_opps 
      ON upa_base.dim_crm_account_id=first_order_opps.dim_crm_account_id
    WHERE dim_crm_opportunity_id IS NOT null
      
  ), final AS (

    SELECT 
      mart_crm_person.dim_crm_person_id,
      mart_crm_person.dim_crm_user_id,
      mart_crm_person.dim_crm_account_id,
      mart_crm_person.mql_date_first_id,
      mart_crm_person.mql_date_first,
      mart_crm_person.mql_datetime_first,
      mart_crm_person.mql_datetime_first_pt,
      mart_crm_person.mql_date_first_pt,
      mart_crm_person.mql_month_first,
      mart_crm_person.mql_month_first_pt,
      mart_crm_person.mql_date_latest,
      mart_crm_person.mql_datetime_latest,
      mart_crm_person.mql_datetime_latest_pt,
      mart_crm_person.mql_date_latest_pt,
      mart_crm_person.mql_month_latest,
      mart_crm_person.mql_month_latest_pt,
      mart_crm_person.created_date,
      mart_crm_person.created_date_pt,
      mart_crm_person.created_month,
      mart_crm_person.created_month_pt,
      mart_crm_person.lead_created_date,
      mart_crm_person.lead_created_date_pt,
      mart_crm_person.lead_created_month,
      mart_crm_person.lead_created_month_pt,
      mart_crm_person.contact_created_date,
      mart_crm_person.contact_created_date_pt,
      mart_crm_person.contact_created_month,
      mart_crm_person.contact_created_month_pt,
      mart_crm_person.true_inquiry_date,
      mart_crm_person.inquiry_date,
      mart_crm_person.inquiry_date_pt,
      mart_crm_person.inquiry_month,
      mart_crm_person.inquiry_month_pt,      
      mart_crm_person.inquiry_inferred_date,
      mart_crm_person.inquiry_inferred_datetime,
      mart_crm_person.inquiry_inferred_date_pt,
      mart_crm_person.inquiry_inferred_month,
      mart_crm_person.inquiry_inferred_month_pt,      
      mart_crm_person.accepted_date,
      mart_crm_person.accepted_datetime,
      mart_crm_person.accepted_datetime_pt,
      mart_crm_person.accepted_date_pt,
      mart_crm_person.accepted_month,
      mart_crm_person.accepted_month_pt,
      mart_crm_person.mql_sfdc_date,
      mart_crm_person.mql_sfdc_datetime,
      mart_crm_person.mql_sfdc_date_pt,
      mart_crm_person.mql_sfdc_month,
      mart_crm_person.mql_sfdc_month_pt,   
      mart_crm_person.mql_inferred_date,
      mart_crm_person.mql_inferred_datetime,
      mart_crm_person.mql_inferred_date_pt,
      mart_crm_person.mql_inferred_month,
      mart_crm_person.mql_inferred_month_pt,
      mart_crm_person.qualifying_date,
      mart_crm_person.qualifying_date_pt,
      mart_crm_person.qualifying_month,
      mart_crm_person.qualifying_month_pt,
      mart_crm_person.qualified_date,
      mart_crm_person.qualified_date_pt,
      mart_crm_person.qualified_month,
      mart_crm_person.qualified_month_pt,
      mart_crm_person.converted_date,
      mart_crm_person.converted_date_pt,
      mart_crm_person.converted_month,
      mart_crm_person.converted_month_pt,
      mart_crm_person.worked_date,
      mart_crm_person.worked_date_pt,
      mart_crm_person.worked_month,
      mart_crm_person.worked_month_pt,
      mart_crm_person.email_domain,
      mart_crm_person.email_domain_type,
      mart_crm_person.email_hash,
      mart_crm_person.status,
      mart_crm_person.lead_source,
      mart_crm_person.source_buckets,
      mart_crm_person.crm_partner_id,
      mart_crm_person.sequence_step_type,
      mart_crm_person.account_demographics_geo AS region,
      mart_crm_person.state,
      mart_crm_person.country,
      mart_crm_person.name_of_active_sequence,
      mart_crm_person.sequence_task_due_date,
      mart_crm_person.sequence_status,
      mart_crm_person.last_activity_date,
      mart_crm_person.is_actively_being_sequenced,
      mart_crm_person.bizible_marketing_channel_path_name,
      mart_crm_person.sales_segment_name,
      mart_crm_person.sales_segment_grouped,
      mart_crm_person.marketo_last_interesting_moment,
      mart_crm_person.marketo_last_interesting_moment_date,
      mart_crm_person.outreach_step_number,
      mart_crm_person.matched_account_owner_role,
      mart_crm_person.matched_account_account_owner_name,
      mart_crm_person.matched_account_sdr_assigned,
      mart_crm_person.matched_account_type,
      mart_crm_person.matched_account_gtm_strategy,
      mart_crm_person.account_demographics_sales_segment,
      mart_crm_person.account_demographics_geo,
      mart_crm_person.account_demographics_region,
      mart_crm_person.account_demographics_area,
      mart_crm_person.account_demographics_territory,
      mart_crm_person.account_demographics_employee_count,
      mart_crm_person.account_demographics_max_family_employee,
      mart_crm_person.account_demographics_upa_country,
      mart_crm_person.account_demographics_upa_state,  
      mart_crm_person.account_demographics_upa_city,
      mart_crm_person.account_demographics_upa_street,
      mart_crm_person.account_demographics_upa_postal_code,
      mart_crm_person.account_demographics_sales_segment_grouped,
      mart_crm_person.account_demographics_segment_region_grouped,
      mart_crm_person.is_mql,
      mart_crm_person.is_inquiry,
      mart_crm_person.is_lead_source_trial,
      mart_crm_opportunity.dim_crm_opportunity_id,
      mart_crm_opportunity.created_date AS opportunity_created_date,
      mart_crm_opportunity.sales_accepted_date,
      mart_crm_opportunity.close_date,
      mart_crm_opportunity.sales_qualified_source_name, 
      mart_crm_opportunity.is_won,
      mart_crm_opportunity.net_arr,
      mart_crm_opportunity.is_edu_oss,
      mart_crm_opportunity.stage_name,
      mart_crm_opportunity.is_sao,
      dim_crm_user.crm_user_sales_segment AS user_sales_segment,
      CASE WHEN is_first_order_available = False THEN mart_crm_opportunity.order_type
           ELSE '1. New - First Order'
      END AS person_order_type,
      dim_crm_user.crm_user_region,
      dim_crm_user.crm_user_area,
      dim_crm_user.crm_user_geo
    FROM mart_crm_person
    LEFT JOIN mart_crm_opportunity 
      ON mart_crm_person.dim_crm_account_id=mart_crm_opportunity.dim_crm_account_id
    LEFT JOIN dim_crm_user 
      ON mart_crm_person.dim_crm_user_id=dim_crm_user.dim_crm_user_id
    LEFT JOIN upa_base 
      ON mart_crm_person.dim_crm_account_id=upa_base.dim_crm_account_id
    LEFT JOIN accounts_with_first_order_opps 
      ON upa_base.dim_parent_crm_account_id = accounts_with_first_order_opps.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rkohnke",
    updated_by="@rkohnke",
    created_date="2022-01-20",
    updated_date="2022-10-07",
  ) }}


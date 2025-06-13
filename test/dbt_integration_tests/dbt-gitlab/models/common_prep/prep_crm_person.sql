{{ config(
    tags=["mnpi_exception"]
) }}

WITH biz_person AS (

    SELECT *
    FROM {{ref('sfdc_bizible_person_source')}}
    WHERE is_deleted = 'FALSE' 

), biz_touchpoints AS (

    SELECT *
    FROM {{ref('sfdc_bizible_touchpoint_source')}}
    WHERE bizible_touchpoint_position LIKE '%FT%'
     AND is_deleted = 'FALSE'

), prep_bizible_touchpoint_information AS (

    SELECT *
    FROM {{ref('prep_bizible_touchpoint_information')}}

), prep_date AS (

    SELECT *
    FROM {{ ref('prep_date') }}

), prep_location_country AS (

    SELECT *
    FROM {{ ref('prep_location_country') }}

), sfdc_account_source AS (

    SELECT
      account_id,
      six_sense_segments
    FROM {{ref('sfdc_account_source')}}

), crm_tasks AS (

    SELECT 
      sfdc_record_id,
      MIN(task_completed_date) AS min_task_completed_date_by_bdr_sdr
    FROM {{ref('prep_crm_task')}}
    WHERE is_deleted = 'FALSE'
    AND task_owner_role LIKE '%BDR%' 
    OR task_owner_role LIKE '%SDR%'
    GROUP BY 1

), crm_events AS (

    SELECT 
      sfdc_record_id,
      MIN(event_date) AS min_task_completed_date_by_bdr_sdr
    FROM {{ref('prep_crm_event')}}
    LEFT JOIN {{ref('dim_crm_user')}} event_user_id 
    ON prep_crm_event.dim_crm_user_id = event_user_id.dim_crm_user_id 
    LEFT JOIN {{ref('dim_crm_user')}}  event_booked_by_id
    ON prep_crm_event.booked_by_employee_number = event_booked_by_id.employee_number
    WHERE 
    event_user_id.user_role_name LIKE '%BDR%' 
    OR event_booked_by_id.user_role_name LIKE '%BDR%' 
    OR event_user_id.user_role_name LIKE '%SDR%' 
    OR event_booked_by_id.user_role_name LIKE '%SDR%' 
    GROUP BY 1

  
), crm_activity_prep AS (
  
    SELECT 
      sfdc_record_id,
      min_task_completed_date_by_bdr_sdr
    FROM crm_tasks
    UNION
    SELECT
      sfdc_record_id,
      min_task_completed_date_by_bdr_sdr
    FROM crm_events
  
),  crm_activity AS (

    SELECT 
      sfdc_record_id,
      MIN(min_task_completed_date_by_bdr_sdr) AS min_task_completed_date_by_bdr_sdr
    FROM crm_activity_prep
    GROUP BY 1 

), biz_person_with_touchpoints AS (

    SELECT

      biz_touchpoints.*,
      biz_person.bizible_contact_id,
      biz_person.bizible_lead_id

    FROM biz_touchpoints
    JOIN biz_person
      ON biz_touchpoints.bizible_person_id = biz_person.person_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY bizible_lead_id,bizible_contact_id ORDER BY bizible_touchpoint_date DESC) = 1

), sfdc_contacts AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_contact_source') }}
    FROM {{ref('sfdc_contact_source')}}
    WHERE is_deleted = 'FALSE'

), sfdc_leads AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_lead_source') }}
    FROM {{ref('sfdc_lead_source')}}
    WHERE is_deleted = 'FALSE'

),  was_converted_lead AS (

    SELECT DISTINCT
      contact_id,
      1 AS was_converted_lead
    FROM {{ ref('sfdc_contact_source') }}
    INNER JOIN {{ ref('sfdc_lead_source') }}
      ON sfdc_contact_source.contact_id = sfdc_lead_source.converted_contact_id

),  marketo_persons AS (

    SELECT
      marketo_lead_id,
      sfdc_type,
      sfdc_lead_id,
      sfdc_contact_id
    FROM {{ ref('marketo_lead_source') }}
    LEFT JOIN {{ ref('marketo_activity_delete_lead_source') }}
      ON marketo_lead_source.marketo_lead_id=marketo_activity_delete_lead_source.lead_id
    WHERE marketo_activity_delete_lead_source.activity_date IS NULL
    QUALIFY ROW_NUMBER() OVER(PARTITION BY sfdc_lead_id,sfdc_contact_id  ORDER BY updated_at DESC) = 1

),  crm_person_final AS (

    SELECT
      --id
      {{ dbt_utils.generate_surrogate_key(['sfdc_contacts.contact_id']) }} AS dim_crm_person_id,
      sfdc_contacts.contact_id                      AS sfdc_record_id,
      bizible_person_id                             AS bizible_person_id,
      'contact'                                     AS sfdc_record_type,
      contact_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,
      marketo_lead_id,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      sfdc_contacts.account_id                      AS dim_crm_account_id,
      reports_to_id,
      owner_id                                      AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      contact_title                                 AS title,
      contact_role                                  AS person_role,
      it_job_title_hierarchy,
      contact_role,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      contact_status                                AS status,
      lead_source,
      lead_source_type,
      inactive_contact,
      was_converted_lead.was_converted_lead         AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      number_of_employees,
      NULL AS company_address_country,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      NULL                                          AS matched_account_owner_role,
      NULL                                          AS matched_account_account_owner_name,
      NULL                                          AS matched_account_sdr_assigned,
      NULL                                          AS matched_account_type,
      NULL                                          AS matched_account_gtm_strategy,
      NULL                                          AS matched_account_bdr_prospecting_status,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      high_priority_datetime,
      CASE
        WHEN high_priority_datetime IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_high_priority,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      mailing_country                               AS country,
      mailing_state                                 AS state,
      last_activity_date,
      NULL                                          AS employee_bucket,
      CASE
        WHEN account_demographics_sales_segment IS NULL OR UPPER(account_demographics_sales_segment) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_sales_segment
      END AS account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      CASE
        WHEN account_demographics_geo IS NULL OR UPPER(account_demographics_geo) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_geo
      END AS account_demographics_geo,
      CASE
        WHEN account_demographics_region IS NULL OR UPPER(account_demographics_region) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_region
      END AS account_demographics_region,
      CASE
        WHEN account_demographics_area IS NULL OR UPPER(account_demographics_area) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_area
      END AS account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      NULL                                          AS crm_partner_id,
      NULL                                          AS ga_client_id,
      NULL                                          AS cognism_company_office_city,
      NULL                                          AS cognism_company_office_state,
      NULL                                          AS cognism_company_office_country,
      NULL                                          AS cognism_city,
      NULL                                          AS cognism_state,
      NULL                                          AS cognism_country,
      cognism_employee_count,
      NULL                                          AS leandata_matched_account_billing_state,
      NULL                                          AS leandata_matched_account_billing_postal_code,
      NULL                                          AS leandata_matched_account_billing_country,
      NULL                                          AS leandata_matched_account_employee_count,
      NULL                                          AS leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      usergem_past_account_id,
      usergem_past_account_type,
      usergem_past_contact_relationship,
      usergem_past_company,
      last_transfer_date_time,
      time_from_last_transfer_to_sequence,
      time_from_mql_to_last_transfer,
      ptp_score_date                                 AS propensity_to_purchase_score_date,
      ptp_score_group                                AS propensity_to_purchase_score_group,
      pql_namespace_creator_job_description,
      pql_namespace_id,
      pql_namespace_name,
      pql_namespace_users,
      is_product_qualified_lead,
      ptp_days_since_trial_start                     AS propensity_to_purchase_days_since_trial_start,
      ptp_insights                                   AS propensity_to_purchase_insights,
      is_ptp_contact,
      ptp_namespace_id                               AS propensity_to_purchase_namespace_id,
      ptp_past_insights                              AS propensity_to_purchase_past_insights,
      ptp_past_score_group                           AS propensity_to_purchase_past_score_group,
      has_account_six_sense_6_qa,
      six_sense_account_6_qa_end_date,
      six_sense_account_6_qa_start_date,
      six_sense_account_buying_stage,
      six_sense_account_profile_fit,
      six_sense_contact_grade                        AS six_sense_person_grade,
      six_sense_contact_profile                      AS six_sense_person_profile,
      six_sense_contact_update_date                  AS six_sense_person_update_date,
      sfdc_account_source.six_sense_segments, 
      mql_worked_by_user_id,
      mql_worked_by_user_manager_id,
      last_worked_by_date,
      last_worked_by_datetime,
      last_worked_by_user_manager_id,
      last_worked_by_user_id,
      groove_active_flows_count,
      groove_added_to_flow_date,
      NULL AS groove_email,
      groove_flow_completed_date,
      is_created_by_groove,
      groove_last_engagement_datetime,
      groove_last_engagement_type,
      groove_last_flow_name,
      groove_last_flow_status,
      groove_last_flow_step_number,
      groove_last_flow_step_type,
      groove_last_step_completed_datetime,
      groove_last_step_skipped,
      groove_last_touch_datetime,
      groove_last_touch_type,
      groove_log_a_call_url,
      groove_next_step_due_date,
      groove_mobile_number,
      groove_phone_number,
      groove_overdue_days,
      groove_removed_from_flow_date,
      groove_removed_from_flow_reason,
      groove_create_opportunity_url,
      NULL AS groove_email_domain,
      groove_engagement_score,
      groove_outbound_email_counter,
      NULL AS is_groove_converted,  
      lead_score_classification,
      is_defaulted_trial,
      NULL                                           AS zoominfo_company_employee_count,
      zoominfo_contact_id,
      NULL                                           AS is_partner_recalled,
      CASE
        WHEN crm_activity.min_task_completed_date_by_bdr_sdr IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_bdr_sdr_worked,
      NULL AS assignment_date,
      NULL AS assignment_type,
      created_date


    FROM sfdc_contacts
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_contacts.contact_id = biz_person_with_touchpoints.bizible_contact_id
    LEFT JOIN was_converted_lead
      ON was_converted_lead.contact_id = sfdc_contacts.contact_id
    LEFT JOIN marketo_persons
      ON sfdc_contacts.contact_id = marketo_persons.sfdc_contact_id and sfdc_type = 'Contact'
    LEFT JOIN crm_activity
      ON sfdc_contacts.contact_id=crm_activity.sfdc_record_id
    LEFT JOIN sfdc_account_source
      ON sfdc_contacts.account_id=sfdc_account_source.account_id

    UNION

    SELECT
      --id
      {{ dbt_utils.generate_surrogate_key(['lead_id']) }} AS dim_crm_person_id,
      lead_id                                    AS sfdc_record_id,
      bizible_person_id                          AS bizible_person_id,
      'lead'                                     AS sfdc_record_type,
      lead_email_hash                            AS email_hash,
      email_domain,
      email_domain_type,
      marketo_lead_id,

      --keys
      master_record_id,
      owner_id,
      record_type_id,
      lean_data_matched_account                  AS dim_crm_account_id,
      NULL                                       AS reports_to_id,
      owner_id                                   AS dim_crm_user_id,

      --info
      person_score,
      behavior_score,
      title,
      NULL                                       AS person_role,
      it_job_title_hierarchy,
      NULL                                       AS contact_role,
      has_opted_out_email,
      email_bounced_date,
      email_bounced_reason,
      lead_status                                AS status,
      lead_source,
      lead_source_type,
      NULL                                       AS inactive_contact,
      0                                          AS was_converted_lead,
      source_buckets,
      net_new_source_categories,
      number_of_employees,
      company_address_country,
      bizible_touchpoint_position,
      bizible_marketing_channel_path,
      bizible_touchpoint_date,
      marketo_last_interesting_moment,
      marketo_last_interesting_moment_date,
      outreach_step_number,
      matched_account_owner_role,
      matched_account_account_owner_name,
      matched_account_sdr_assigned,
      matched_account_type,
      matched_account_gtm_strategy,
      matched_account_bdr_prospecting_status,
      is_first_order_initial_mql,
      is_first_order_mql,
      is_first_order_person,
      last_utm_content,
      last_utm_campaign,
      sequence_step_type,
      name_of_active_sequence,
      sequence_task_due_date,
      sequence_status,
      is_actively_being_sequenced,
      high_priority_datetime, 
      CASE
        WHEN high_priority_datetime IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_high_priority,
      prospect_share_status,
      partner_prospect_status,
      partner_prospect_id,
      partner_prospect_owner_name,
      country,
      state,
      last_activity_date,
      employee_bucket,
      CASE
        WHEN account_demographics_sales_segment IS NULL OR UPPER(account_demographics_sales_segment) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_sales_segment
      END AS account_demographics_sales_segment,
      account_demographics_sales_segment_grouped,
      CASE
        WHEN account_demographics_geo IS NULL OR UPPER(account_demographics_geo) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_geo
      END AS account_demographics_geo,
      CASE
        WHEN account_demographics_region IS NULL OR UPPER(account_demographics_region) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_region
      END AS account_demographics_region,
      CASE
        WHEN account_demographics_area IS NULL OR UPPER(account_demographics_area) LIKE '%NOT FOUND%'
          THEN 'UNKNOWN'
        ELSE account_demographics_area
      END AS account_demographics_area,
      account_demographics_segment_region_grouped,
      account_demographics_territory,
      account_demographics_employee_count,
      account_demographics_max_family_employee,
      account_demographics_upa_country,
      account_demographics_upa_state,
      account_demographics_upa_city,
      account_demographics_upa_street,
      account_demographics_upa_postal_code,
      crm_partner_id,
      ga_client_id,
      cognism_company_office_city,
      cognism_company_office_state,
      cognism_company_office_country,
      cognism_city,
      cognism_state,
      cognism_country,
      cognism_employee_count,
      leandata_matched_account_billing_state,
      leandata_matched_account_billing_postal_code,
      leandata_matched_account_billing_country,
      leandata_matched_account_employee_count,
      leandata_matched_account_sales_segment,
      zoominfo_contact_city,
      zoominfo_contact_state,
      zoominfo_contact_country,
      zoominfo_company_city,
      zoominfo_company_state,
      zoominfo_company_country,
      zoominfo_phone_number, 
      zoominfo_mobile_phone_number,
      zoominfo_do_not_call_direct_phone,
      zoominfo_do_not_call_mobile_phone,
      traction_first_response_time,
      traction_first_response_time_seconds,
      traction_response_time_in_business_hours,
      usergem_past_account_id,
      usergem_past_account_type,
      usergem_past_contact_relationship,
      usergem_past_company,
      last_transfer_date_time,
      time_from_last_transfer_to_sequence,
      time_from_mql_to_last_transfer,
      ptp_score_date                                 AS propensity_to_purchase_score_date,
      ptp_score_group                                AS propensity_to_purchase_score_group,
      pql_namespace_creator_job_description,
      pql_namespace_id,
      pql_namespace_name,
      pql_namespace_users,
      is_product_qualified_lead,
      ptp_days_since_trial_start                     AS propensity_to_purchase_days_since_trial_start,
      ptp_insights                                   AS propensity_to_purchase_insights,
      is_ptp_contact,
      ptp_namespace_id                               AS propensity_to_purchase_namespace_id,
      ptp_past_insights                              AS propensity_to_purchase_past_insights,
      ptp_past_score_group                           AS propensity_to_purchase_past_score_group,
      has_account_six_sense_6_qa,
      six_sense_account_6_qa_end_date,
      six_sense_account_6_qa_start_date,
      six_sense_account_buying_stage,
      six_sense_account_profile_fit,
      six_sense_lead_grade,
      six_sense_lead_profile_fit,
      six_sense_lead_update_date,
      six_sense_segments,
      mql_worked_by_user_id,
      mql_worked_by_user_manager_id,
      last_worked_by_date,
      last_worked_by_datetime,
      last_worked_by_user_manager_id,
      last_worked_by_user_id,
      groove_active_flows_count,
      groove_added_to_flow_date,
      NULL AS groove_email,
      groove_flow_completed_date,
      is_created_by_groove,
      groove_last_engagement_datetime,
      groove_last_engagement_type,
      groove_last_flow_name,
      groove_last_flow_status,
      groove_last_flow_step_number,
      groove_last_flow_step_type,
      groove_last_step_completed_datetime,
      groove_last_step_skipped,
      groove_last_touch_datetime,
      groove_last_touch_type,
      groove_log_a_call_url,
      groove_next_step_due_date,
      groove_mobile_number,
      groove_phone_number,
      groove_overdue_days,
      groove_removed_from_flow_date,
      groove_removed_from_flow_reason,
      NULL AS groove_create_opportunity_url,
      NULL AS groove_email_domain,
      NULL AS groove_engagement_score,
      NULL AS groove_outbound_email_counter,
      is_groove_converted,
      lead_score_classification,
      is_defaulted_trial,
      zoominfo_company_employee_count,
      NULL AS zoominfo_contact_id,
      is_partner_recalled,
      CASE
        WHEN crm_tasks.min_task_completed_date_by_bdr_sdr IS NOT NULL
          THEN TRUE
        ELSE FALSE
      END AS is_bdr_sdr_worked,
      assignment_date,
      assignment_type,
      created_date

    FROM sfdc_leads
    LEFT JOIN biz_person_with_touchpoints
      ON sfdc_leads.lead_id = biz_person_with_touchpoints.bizible_lead_id
    LEFT JOIN marketo_persons
      ON sfdc_leads.lead_id = marketo_persons.sfdc_lead_id and sfdc_type = 'Lead'
    LEFT JOIN crm_tasks
      ON sfdc_leads.lead_id=crm_tasks.sfdc_record_id
    WHERE is_converted = 'FALSE'

), final AS (

    SELECT
      crm_person_final.*,
      LOWER(COALESCE(
                     account_demographics_upa_country,
                     zoominfo_company_country,
                     country
                    )
       ) AS two_letter_person_first_country,
      CASE
        -- remove a few case where value is only numbers
        WHEN (TRY_TO_NUMBER(two_letter_person_first_country)) IS NOT NULL THEN
             NULL
        WHEN LEN(two_letter_person_first_country) = 2 AND prep_location_country.country_name IS NOT NULL THEN
            INITCAP(prep_location_country.country_name)
        WHEN LEN(two_letter_person_first_country) = 2 THEN
            -- This condition would be for a country code that isn't on the model
            UPPER(two_letter_person_first_country)
        ELSE
            INITCAP(two_letter_person_first_country)
      END AS person_first_country,
      prep_date.fiscal_year  AS created_date_fiscal_year,
      CONCAT(
        UPPER(crm_person_final.account_demographics_sales_segment),
        '-',
        UPPER(crm_person_final.account_demographics_geo),
        '-',
        UPPER(crm_person_final.account_demographics_region),
        '-',
        UPPER(crm_person_final.account_demographics_area),
        '-',
        prep_date.fiscal_year
      ) AS dim_account_demographics_hierarchy_sk,
      CASE
        WHEN LOWER(email_domain) = 'gitlab.com'
          THEN TRUE
        WHEN LOWER(account_demographics_geo) = 'jihu'
          THEN TRUE
        ELSE FALSE
      END AS is_exclude_from_reporting,

    --MQL and Most Recent Touchpoint info
      prep_bizible_touchpoint_information.bizible_mql_touchpoint_id,
      prep_bizible_touchpoint_information.bizible_mql_touchpoint_date,
      prep_bizible_touchpoint_information.bizible_mql_form_url,
      prep_bizible_touchpoint_information.bizible_mql_sfdc_campaign_id,
      prep_bizible_touchpoint_information.bizible_mql_ad_campaign_name,
      prep_bizible_touchpoint_information.bizible_mql_marketing_channel,
      prep_bizible_touchpoint_information.bizible_mql_marketing_channel_path,
      prep_bizible_touchpoint_information.bizible_most_recent_touchpoint_id,
      prep_bizible_touchpoint_information.bizible_most_recent_touchpoint_date,
      prep_bizible_touchpoint_information.bizible_most_recent_form_url,
      prep_bizible_touchpoint_information.bizible_most_recent_sfdc_campaign_id,
      prep_bizible_touchpoint_information.bizible_most_recent_ad_campaign_name,
      prep_bizible_touchpoint_information.bizible_most_recent_marketing_channel,
      prep_bizible_touchpoint_information.bizible_most_recent_marketing_channel_path
    FROM crm_person_final
    LEFT JOIN prep_date
      ON prep_date.date_actual = crm_person_final.created_date::DATE
    LEFT JOIN prep_bizible_touchpoint_information
      ON crm_person_final.sfdc_record_id=prep_bizible_touchpoint_information.sfdc_record_id
    LEFT JOIN prep_location_country
      ON two_letter_person_first_country = LOWER(prep_location_country.iso_2_country_code)
      -- Only join when the value is 2 letters
      AND LEN(two_letter_person_first_country) = 2
    WHERE crm_person_final.sfdc_record_id != '00Q4M00000kDDKuUAO' --DQ issue: https://gitlab.com/gitlab-data/analytics/-/issues/11559

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-12-08",
    updated_date="2025-03-03"
) }}

WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'contact') }}



),

renamed AS (

  SELECT
    -- id
    id AS contact_id,
    name AS contact_name,
    firstname AS contact_first_name,
    lastname AS contact_last_name,
    email AS contact_email,
    SPLIT_PART(email, '@', 2) AS email_domain,
    {{ email_domain_type("split_part(email,'@',2)", 'leadsource') }} AS email_domain_type,

    -- keys
    accountid AS account_id,
    masterrecordid AS master_record_id,
    ownerid AS owner_id,
    recordtypeid AS record_type_id,
    reportstoid AS reports_to_id,

    --contact info
    title AS contact_title,
    {{ it_job_title_hierarchy('title') }},
    role__c AS contact_role,
    mobilephone AS mobile_phone,
    mkto71_lead_score__c AS person_score,

    department AS department,
    contact_status__c AS contact_status,
    requested_contact__c AS requested_contact,
    inactive_contact__c AS inactive_contact,
    hasoptedoutofemail AS has_opted_out_email,
    invalid_email_address__c AS invalid_email_address,
    isemailbounced AS email_is_bounced,
    emailbounceddate AS email_bounced_date,
    emailbouncedreason AS email_bounced_reason,

    mailingstreet AS mailing_address,
    mailingcity AS mailing_city,
    mailingstate AS mailing_state,
    mailingstatecode AS mailing_state_code,
    mailingcountry AS mailing_country,
    mailingcountrycode AS mailing_country_code,
    mailingpostalcode AS mailing_zip_code,

    -- info
    dozisf__zoominfo_company_id__c AS zoominfo_company_id,
    dozisf__zoominfo_id__c AS zoominfo_contact_id,
    zi_company_revenue__c AS zoominfo_company_revenue,
    zi_employee_count__c AS zoominfo_company_employee_count,
    cognism_number_of_employees__c AS cognism_employee_count,
    employees__c AS number_of_employees,
    zi_contact_city__c AS zoominfo_contact_city,
    zi_company_city__c AS zoominfo_company_city,
    zi_industry__c AS zoominfo_company_industry,
    zi_company_state__c AS zoominfo_company_state,
    zi_contact_state__c AS zoominfo_contact_state,
    zi_company_country__c AS zoominfo_company_country,
    zi_contact_country__c AS zoominfo_contact_country,
    zi_phone_number__c AS zoominfo_phone_number, 
    zi_mobile_phone_number__c AS zoominfo_mobile_phone_number,
    zi_do_not_call_direct_phone__c AS zoominfo_do_not_call_direct_phone,
    zi_do_not_call_mobile_phone__c AS zoominfo_do_not_call_mobile_phone,
    using_ce__c AS using_ce,
    ee_trial_start_date__c AS ee_trial_start_date,
    ee_trial_end_date__c AS ee_trial_end_date,
    industry__c AS industry,
    -- maybe we can exclude this if it's not relevant
    responded_to_githost_price_change__c AS responded_to_githost_price_change,
    core_check_in_notes__c AS core_check_in_notes,
    leadsource AS lead_source,
    lead_source_type__c AS lead_source_type,
    behavior_score__c AS behavior_score,
    outreach_stage__c AS outreach_stage,
    sequence_step_number__c AS outreach_step_number,
    account_type__c AS account_type,
    contact_assigned_datetime__c::TIMESTAMP AS assigned_datetime,
    mql_timestamp__c AS marketo_qualified_lead_timestamp,
    mql_datetime__c AS marketo_qualified_lead_datetime,
    mql_date__c AS marketo_qualified_lead_date,
    mql_datetime_inferred__c AS mql_datetime_inferred,
    inquiry_datetime__c AS inquiry_datetime,
    inquiry_datetime_inferred__c AS inquiry_datetime_inferred,
    accepted_datetime__c AS accepted_datetime,
    qualifying_datetime__c AS qualifying_datetime,
    qualified_datetime__c AS qualified_datetime,
    unqualified_datetime__c AS unqualified_datetime,
    nurture_datetime__c AS initial_recycle_datetime,
    initial_nurture_datetime__c AS most_recent_recycle_datetime,
    bad_data_datetime__c AS bad_data_datetime,
    worked_date__c AS worked_datetime,
    web_portal_purchase_datetime__c AS web_portal_purchase_datetime,
    mkto_si__last_interesting_moment__c AS marketo_last_interesting_moment,
    mkto_si__last_interesting_moment_date__c AS marketo_last_interesting_moment_date,
    last_utm_campaign__c AS last_utm_campaign,
    last_utm_content__c AS last_utm_content,
    vartopiadrs__partner_prospect_acceptance__c AS prospect_share_status,
    vartopiadrs__partner_prospect_status__c AS partner_prospect_status,
    vartopiadrs__vartopia_prospect_id__c AS partner_prospect_id,
    vartopiadrs__partner_prospect_owner_name__c AS partner_prospect_owner_name,
    sequence_step_type2__c AS sequence_step_type,
    name_of_active_sequence__c AS name_of_active_sequence,
    sequence_task_due_date__c::DATE AS sequence_task_due_date,
    sequence_status__c AS sequence_status,
    actively_being_sequenced__c::BOOLEAN AS is_actively_being_sequenced,
    fo_initial_mql__c AS is_first_order_initial_mql,
    fo_mql__c AS is_first_order_mql,
    is_first_order_person__c AS is_first_order_person,
    true_initial_mql_date__c AS true_initial_mql_date,
    true_mql_date__c AS true_mql_date,
    initial_mql_date__c AS initial_marketo_mql_date_time,
	  last_transfer_date_time__c AS last_transfer_date_time,
	  time_from_last_transfer_to_sequence__c AS time_from_last_transfer_to_sequence,
	  time_from_mql_to_last_transfer__c AS time_from_mql_to_last_transfer,
    high_priority__c AS is_high_priority,
    high_priority_timestamp__c AS high_priority_datetime,
    ptp_score_date__c AS ptp_score_date,
    ptp_score_group__c AS ptp_score_group,
    pqlnamespacecreatorjobdescription__c AS pql_namespace_creator_job_description,
    pql_namespace_id__c AS pql_namespace_id,
    pql_namespace_name__c AS pql_namespace_name,
    pqlnamespaceusers__c AS pql_namespace_users,
    pql_product_qualified_lead__c AS is_product_qualified_lead,
    ptp_days_since_trial_start__c AS ptp_days_since_trial_start,
    ptp_insights__c AS ptp_insights,
    ptp_is_ptp_contact__c AS is_ptp_contact,
    ptp_namespace_id__c AS ptp_namespace_id,
    ptp_past_insights__c AS ptp_past_insights,
    ptp_past_score_group__c AS ptp_past_score_group,
    lead_score_classification__c AS lead_score_classification,
    is_defaulted_trial__c as is_defaulted_trial,
    {{ sfdc_source_buckets('leadsource') }}

    -- worked by
    mql_worked_by_user_name__c AS mql_worked_by_user_id,
    mql_worked_by_user_manager_name__c AS mql_worked_by_user_manager_id,
    last_worked_by_date_time__c::DATE AS last_worked_by_date,
    last_worked_by_date_time__c::TIMESTAMP AS last_worked_by_datetime,
    last_worked_by_user_manager_name__c AS last_worked_by_user_manager_id,
    last_worked_by_user_name__c AS last_worked_by_user_id,

    -- account demographics fields
    account_demographics_sales_segment__c AS account_demographics_sales_segment,
    CASE
      WHEN account_demographics_sales_segment__c IN ('Large', 'PubSec') THEN 'Large'
      ELSE account_demographics_sales_segment__c
    END AS account_demographics_sales_segment_grouped,
    account_demographics_geo__c AS account_demographics_geo,
    account_demographics_region__c AS account_demographics_region,
    account_demographics_area__c AS account_demographics_area,
    {{ sales_segment_region_grouped('account_demographics_sales_segment__c', 'account_demographics_geo__c', 'account_demographics_region__c') }}
    AS account_demographics_segment_region_grouped,
    account_demographics_territory__c AS account_demographics_territory,
    account_demographic_employee_count__c AS account_demographics_employee_count,
    account_demographics_max_family_employe__c AS account_demographics_max_family_employee,
    account_demographics_upa_country__c AS account_demographics_upa_country,
    account_demographics_upa_state__c AS account_demographics_upa_state,
    account_demographics_upa_city__c AS account_demographics_upa_city,
    account_demographics_upa_street__c AS account_demographics_upa_street,
    account_demographics_upa_postal_code__c AS account_demographics_upa_postal_code,

    --path factory info
    pathfactory_experience_name__c AS pathfactory_experience_name,
    pathfactory_engagement_score__c AS pathfactory_engagement_score,
    pathfactory_content_count__c AS pathfactory_content_count,
    pathfactory_content_list__c AS pathfactory_content_list,
    pathfactory_content_journey__c AS pathfactory_content_journey,
    pathfactory_topic_list__c AS pathfactory_topic_list,

    --6 Sense Fields
    x6sense_account_6qa__c::BOOLEAN AS has_account_six_sense_6_qa,
    x6sense_account_6qa_end_date__c::DATE AS six_sense_account_6_qa_end_date,
    x6sense_account_6qa_start_date__c::DATE AS six_sense_account_6_qa_start_date,
    x6sense_account_buying_stage__c AS six_sense_account_buying_stage,
    x6sense_account_profile_fit__c AS six_sense_account_profile_fit,
    x6sense_contact_grade__c AS six_sense_contact_grade,
    x6sense_contact_profile__c AS six_sense_contact_profile,
    x6sense_contact_update_date__c::DATE AS six_sense_contact_update_date,

    --Traction Fields
    tracrtc__first_response_time_start__c AS traction_first_response_time,
    tracrtc__first_response_time_in_seconds__c AS traction_first_response_time_seconds,
    tracrtc__response_time_within_business_hours__c AS traction_response_time_in_business_hours,

    --UserGems
    usergem__pastaccount__c AS usergem_past_account_id,
    usergem__pastaccounttype__c AS usergem_past_account_type,
    usergem__pastcontactrelationship__c AS usergem_past_contact_relationship,
    usergem__pastcompany__c AS usergem_past_company,

    --Groove
    groove_active_flows_count__c AS groove_active_flows_count,
    groove_added_to_flow_date__c::DATE AS groove_added_to_flow_date,
    groove_flow_completed_date__c::DATE AS groove_flow_completed_date,
    dascoopcomposer__is_created_by_groove__c AS is_created_by_groove,
    groove_last_engagement__c AS groove_last_engagement_datetime,
    groove_last_engagement_type__c AS groove_last_engagement_type,
    groove_last_flow_name__c AS groove_last_flow_name,
    groove_last_flow_status__c AS groove_last_flow_status,
    groove_last_flow_step_number__c AS groove_last_flow_step_number,
    groove_last_flow_step_type__c AS groove_last_flow_step_type,
    groove_last_step_completed__c AS groove_last_step_completed_datetime,
    groove_last_step_skipped__c AS groove_last_step_skipped,
    groove_last_touch__c AS groove_last_touch_datetime,
    groove_last_touch_type__c AS groove_last_touch_type,
    dascoopcomposer__groove_log_a_call__c AS groove_log_a_call_url,
    groove_next_step_due_date__c::DATE AS groove_next_step_due_date,
    dascoopcomposer__normalized_mobile__c AS groove_mobile_number,
    dascoopcomposer__normalized_phone__c AS groove_phone_number,
    groove_overdue_days__c AS groove_overdue_days,
    groove_removed_from_flow_date__c::DATE AS groove_removed_from_flow_date,
    groove_removed_from_flow_reason__c AS groove_removed_from_flow_reason,
    dascoopcomposer__groove_create_opportunity__c AS groove_create_opportunity_url,
    groove_engagement_score__c AS groove_engagement_score,
    groove_outbound_email_counter__c AS groove_outbound_email_counter,

    --gl info
    account_owner__c AS account_owner,
    ae_comments__c AS ae_comments,
    business_development_rep__c AS business_development_rep_name,
    outbound_bdr__c AS outbound_business_development_rep_name,

    -- metadata
    createdbyid AS created_by_id,
    createddate AS created_date,
    isdeleted AS is_deleted,
    lastactivitydate::DATE AS last_activity_date,
    lastcurequestdate AS last_cu_request_date,
    lastcuupdatedate AS last_cu_update_date,
    lastmodifiedbyid AS last_modified_by_id,
    lastmodifieddate AS last_modified_date,
    systemmodstamp

  FROM source

)

SELECT *
FROM renamed

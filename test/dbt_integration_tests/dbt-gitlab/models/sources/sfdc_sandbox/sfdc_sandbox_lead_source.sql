WITH source AS (

  SELECT *
  FROM {{ source('salesforce_sandbox', 'lead') }}

),

renamed AS (

  SELECT
    --id
    id AS lead_id,
    name AS lead_name,
    firstname AS lead_first_name,
    lastname AS lead_last_name,
    email AS lead_email,
    SPLIT_PART(email, '@', 2) AS email_domain,
    {{ email_domain_type("split_part(email,'@',2)", 'leadsource') }} AS email_domain_type,

    --keys
    masterrecordid AS master_record_id,
    convertedaccountid AS converted_account_id,
    convertedcontactid AS converted_contact_id,
    convertedopportunityid AS converted_opportunity_id,
    ownerid AS owner_id,
    recordtypeid AS record_type_id,
    round_robin_id__c AS round_robin_id,
    instance_uuid__c AS instance_uuid,
    lean_data_matched_account__c AS lean_data_matched_account,

    --lead info
    isconverted AS is_converted,
    converteddate AS converted_date,
    title AS title,
    {{ it_job_title_hierarchy('title') }},
    donotcall AS is_do_not_call,
    hasoptedoutofemail AS has_opted_out_email,
    emailbounceddate AS email_bounced_date,
    emailbouncedreason AS email_bounced_reason,
    behavior_score__c AS behavior_score,
    leadsource AS lead_source,
    lead_from__c AS lead_from,
    lead_source_type__c AS lead_source_type,
    lead_conversion_approval_status__c AS lead_conversion_approval_status,
    street AS street,
    city AS city,
    state AS state,
    statecode AS state_code,
    country AS country,
    countrycode AS country_code,
    postalcode AS postal_code,
    zi_company_country__c AS zoominfo_company_country,
    zi_contact_country__c AS zoominfo_contact_country,
    zi_company_state__c AS zoominfo_company_state,
    zi_contact_state__c AS zoominfo_contact_state,

    -- info
    requested_contact__c AS requested_contact,
    company AS company,
    dozisf__zoominfo_company_id__c AS zoominfo_company_id,
    zi_company_revenue__c AS zoominfo_company_revenue,
    zi_employee_count__c AS zoominfo_company_employee_count,
    zi_contact_city__c AS zoominfo_contact_city,
    zi_company_city__c AS zoominfo_company_city,
    zi_industry__c AS zoominfo_company_industry,
    zi_phone_number__c AS zoominfo_phone_number,
    zi_mobile_phone_number__c AS zoominfo_mobile_phone_number,
    zi_do_not_call_direct_phone__c AS zoominfo_do_not_call_direct_phone,
    zi_do_not_call_mobile_phone__c AS zoominfo_do_not_call_mobile_phone,
    buying_process_for_procuring_gitlab__c AS buying_process,
    core_check_in_notes__c AS core_check_in_notes,
    industry AS industry,
    largeaccount__c AS is_large_account,
    outreach_stage__c AS outreach_stage,
    sequence_step_number__c AS outreach_step_number,
    interested_in_gitlab_ee__c AS is_interested_gitlab_ee,
    interested_in_hosted_solution__c AS is_interested_in_hosted,
    lead_assigned_datetime__c::TIMESTAMP AS assigned_datetime,
    matched_account_top_list__c AS matched_account_top_list,
    matched_account_owner_role__c AS matched_account_owner_role,
    matched_account_sdr_assigned__c AS matched_account_sdr_assigned,
    matched_account_gtm_strategy__c AS matched_account_gtm_strategy,
    NULL AS matched_account_bdr_prospecting_status,
    engagio__matched_account_type__c AS matched_account_type,
    engagio__matched_account_owner_name__c AS matched_account_account_owner_name,
    mql_datetime__c AS marketo_qualified_lead_datetime,
    mql_datetime_inferred__c AS mql_datetime_inferred,
    inquiry_datetime__c AS inquiry_datetime,
    inquiry_datetime_inferred__c AS inquiry_datetime_inferred,
    accepted_datetime__c AS accepted_datetime,
    qualifying_datetime__c AS qualifying_datetime,
    qualified_datetime__c AS qualified_datetime,
    unqualified_datetime__c AS unqualified_datetime,
    nurture_datetime__c AS nurture_datetime,
    bad_data_datetime__c AS bad_data_datetime,
    worked_date__c AS worked_datetime,
    web_portal_purchase_datetime__c AS web_portal_purchase_datetime,
    {{ sales_segment_cleaning('sales_segmentation__c') }} AS sales_segmentation,
    mkto71_lead_score__c AS person_score,
    status AS lead_status,
    last_utm_campaign__c AS last_utm_campaign,
    last_utm_content__c AS last_utm_content,
    crm_partner_id_lookup__c AS crm_partner_id,
    vartopiadrs__partner_prospect_acceptance__c AS prospect_share_status,
    vartopiadrs__partner_prospect_status__c AS partner_prospect_status,
    vartopiadrs__vartopia_prospect_id__c AS partner_prospect_id,
    vartopiadrs__partner_prospect_owner_name__c AS partner_prospect_owner_name,
    name_of_active_sequence__c AS name_of_active_sequence,
    sequence_task_due_date__c::DATE AS sequence_task_due_date,
    sequence_status__c AS sequence_status,
    sequence_step_type2__c AS sequence_step_type,
    actively_being_sequenced__c::BOOLEAN AS is_actively_being_sequenced,
    gaclientid__c AS ga_client_id,
    employee_buckets__c AS employee_bucket,
    fo_initial_mql__c AS is_first_order_initial_mql,
    fo_mql__c AS is_first_order_mql,
    is_first_order_person__c AS is_first_order_person,
    true_initial_mql_date__c AS true_initial_mql_date,
    true_mql_date__c AS true_mql_date,
    last_transfer_date_time__c AS last_transfer_date_time,
    NULL AS initial_marketo_mql_date_time,
	  time_from_last_transfer_to_sequence__c AS time_from_last_transfer_to_sequence,
	  time_from_mql_to_last_transfer__c AS time_from_mql_to_last_transfer,
    NULL AS is_high_priority,
    NULL AS pql_namespace_creator_job_description,
    NULL AS pql_namespace_id,
    NULL AS pql_namespace_name,
    NULL AS pql_namespace_users,
    pql_product_qualified_lead__c AS is_product_qualified_lead,
    ptp_is_ptp_contact__c AS is_ptp_contact,

    {{ sfdc_source_buckets('leadsource') }}

    -- territory success planning info
    leandata_owner__c AS tsp_owner,
    leandata_region__c AS tsp_region,
    leandata_sub_region__c AS tsp_sub_region,
    leandata_territory__c AS tsp_territory,

    -- account demographics fields
    account_demographics_sales_segment_2__c AS account_demographics_sales_segment,
    account_demographics_sales_segment__c AS account_demographics_sales_segment_deprecated,
    CASE
      WHEN account_demographics_sales_segment_2__c IN ('Large', 'PubSec') THEN 'Large'
      ELSE account_demographics_sales_segment_2__c
    END AS account_demographics_sales_segment_grouped,
    account_demographics_geo__c AS account_demographics_geo,
    account_demographics_region__c AS account_demographics_region,
    account_demographics_area__c AS account_demographics_area,
    {{ sales_segment_region_grouped('account_demographics_sales_segment_2__c', 'account_demographics_geo__c', 'account_demographics_region__c') }}
    AS account_demographics_segment_region_grouped,
    account_demographics_territory__c AS account_demographics_territory,
    account_demographics_employee_count__c AS account_demographics_employee_count,
    account_demographics_max_family_employe__c AS account_demographics_max_family_employee,
    account_demographics_upa_state__c AS account_demographics_upa_state,
    account_demographics_upa_city__c AS account_demographics_upa_city,
    account_demographics_upa_postal_code__c AS account_demographics_upa_postal_code,

    --marketo sales insight
    mkto_si__last_interesting_moment_desc__c AS marketo_last_interesting_moment,
    mkto_si__last_interesting_moment_date__c AS marketo_last_interesting_moment_date,

    --gitlab internal
    bdr_lu__c AS business_development_look_up,
    business_development_rep_contact__c AS business_development_representative_contact,
    business_development_representative__c AS business_development_representative,
    sdr_lu__c AS crm_sales_dev_rep_id,
    competition__c AS competition,

    -- Cognism Data
    cognism_company_office_city__c AS cognism_company_office_city,
    cognism_company_office_state__c AS cognism_company_office_state,
    cognism_company_office_country__c AS cognism_company_office_country,
    cognism_city__c as cognism_city,
    cognism_state__c as cognism_state,
    cognism_country__c as cognism_country,
    cognism_number_of_employees__c AS cognism_employee_count,

    --LeanData
    NULL AS leandata_matched_account_billing_state,
    NULL AS leandata_matched_account_billing_postal_code,
    NULL AS leandata_matched_account_billing_country,
    NULL AS leandata_matched_account_employee_count,


    --metadata
    createdbyid AS created_by_id,
    createddate AS created_date,
    isdeleted AS is_deleted,
    lastactivitydate::DATE AS last_activity_date,
    lastmodifiedbyid AS last_modified_id,
    lastmodifieddate AS last_modified_date,
    systemmodstamp

  FROM source

)

SELECT *
FROM renamed

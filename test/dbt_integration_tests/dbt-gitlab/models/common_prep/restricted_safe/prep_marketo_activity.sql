WITH marketo_person AS (

  SELECT DISTINCT
    dim_marketo_person_id,
    dim_crm_person_id
  FROM {{ ref('prep_marketo_person') }}

),

prep_crm_person AS (

  SELECT
    marketo_lead_id,
    dim_crm_person_id,
    sfdc_record_id,
    email_domain_type
  FROM {{ ref('prep_crm_person') }}

),

prep_campaign AS (

  SELECT DISTINCT
    campaign_name,
    CASE
      WHEN campaign_name = '20200420_Enterprise_CODE_VE'
        THEN 'Webcast'
      ELSE type
    END AS type
  FROM {{ ref('prep_campaign') }}
  WHERE status != 'Aborted'

), 

dim_marketing_contact AS (

  SELECT
        marketo_lead_id,
        sfdc_record_id,
        CASE
            WHEN MAX(bdg_marketing_contact_order.is_setup_for_company) = TRUE
              THEN TRUE
            ELSE FALSE
        END AS has_namespace_setup_for_company_use
    FROM {{ ref('dim_marketing_contact_no_pii') }}
    LEFT JOIN {{ ref('bdg_marketing_contact_order') }}
        ON dim_marketing_contact_no_pii.dim_marketing_contact_id = bdg_marketing_contact_order.dim_marketing_contact_id
    GROUP BY 1,2

), 

marketo_activity_change_score_source AS (

  SELECT *
  FROM {{ ref('marketo_activity_change_score_source') }}

),

add_to_campaign AS (

  SELECT
    marketo_activity_add_to_sfdc_campaign_source.marketo_activity_add_to_sfdc_campaign_id
                                                                            AS dim_marketo_activity_id,
    marketo_activity_add_to_sfdc_campaign_source.lead_id                    AS dim_marketo_person_id,
    marketo_activity_add_to_sfdc_campaign_source.activity_date::TIMESTAMP   AS activity_datetime,
    marketo_activity_add_to_sfdc_campaign_source.activity_date::DATE        AS activity_date,
    marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value    AS campaign_name,
    marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value_id AS campaign_id,
    marketo_activity_add_to_sfdc_campaign_source.campaign_id                AS marketo_campaign_id,
    prep_campaign.type                                                      AS campaign_type,
    marketo_activity_add_to_sfdc_campaign_source.status                     AS campaign_member_status
  FROM {{ ref('marketo_activity_add_to_sfdc_campaign_source') }}
  LEFT JOIN prep_campaign
    ON marketo_activity_add_to_sfdc_campaign_source.primary_attribute_value = prep_campaign.campaign_name

),

change_campaign_status AS (

  SELECT
    marketo_activity_change_status_in_sfdc_campaign_source.marketo_activity_change_status_in_sfdc_campaign_id
                                                                                      AS dim_marketo_activity_id,
    marketo_activity_change_status_in_sfdc_campaign_source.lead_id                    AS dim_marketo_person_id,
    marketo_activity_change_status_in_sfdc_campaign_source.activity_date::TIMESTAMP   AS activity_datetime,
    marketo_activity_change_status_in_sfdc_campaign_source.activity_date::DATE        AS activity_date,
    marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value    AS campaign_name,
    marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value_id AS campaign_id,
    marketo_activity_change_status_in_sfdc_campaign_source.campaign_id                AS marketo_campaign_id,
    prep_campaign.type                                                                AS campaign_type,
    marketo_activity_change_status_in_sfdc_campaign_source.new_status                 AS campaign_member_status
  FROM {{ ref('marketo_activity_change_status_in_sfdc_campaign_source') }}
  LEFT JOIN prep_campaign
    ON marketo_activity_change_status_in_sfdc_campaign_source.primary_attribute_value = prep_campaign.campaign_name

),

combined_campaign AS (

  SELECT *
  FROM add_to_campaign
  UNION
  SELECT *
  FROM change_campaign_status

),

combined_campaigns_with_activity_type AS (

  SELECT
    combined_campaign.*,
    {{marketo_campaign_activity_type(
      'campaign_type',
      'campaign_member_status',
      'campaign_name'
      ) 
    }}
  FROM combined_campaign

),

marketo_form_fill AS (

  SELECT 
    {{ hash_sensitive_columns('marketo_activity_fill_out_form_source') }},
    {{form_ids_type(
        'primary_attribute_value'
        ) 
      }}
  FROM {{ ref('marketo_activity_fill_out_form_source') }}

),

inbound_forms_high AS (

  SELECT
    marketo_activity_fill_out_form_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_form_name,
    primary_attribute_value_id AS marketo_form_id,
    form_fields,
    webpage_id                 AS marketo_webpage_id,
    query_parameters           AS marketo_query_parameters,
    referrer_url               AS marketo_referrer_url,
    client_ip_address_hash     AS marketo_form_client_ip_address_hash,
    campaign_id                AS marketo_campaign_id,
    'Inbound Request: High'    AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Inbound - High'           AS scored_action
  FROM marketo_form_fill
  WHERE form_ids_type = 'Inbound - High'

),

subscription AS (

  SELECT
    marketo_activity_fill_out_form_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_form_name,
    primary_attribute_value_id AS marketo_form_id,
    form_fields,
    webpage_id                 AS marketo_webpage_id,
    query_parameters           AS marketo_query_parameters,
    referrer_url               AS marketo_referrer_url,
    client_ip_address_hash     AS marketo_form_client_ip_address_hash,
    campaign_id                AS marketo_campaign_id,
    'Form - Subscription'      AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Subscription'             AS scored_action
  FROM marketo_form_fill
  WHERE form_ids_type = 'Subscription'

),

self_managed_trial AS (

  SELECT
    marketo_form_fill.marketo_activity_fill_out_form_id
                                                  AS dim_marketo_activity_id,
    marketo_form_fill.lead_id                     AS dim_marketo_person_id,
    marketo_form_fill.activity_date::TIMESTAMP    AS activity_datetime,
    marketo_form_fill.activity_date::DATE         AS activity_date,
    marketo_form_fill.primary_attribute_value     AS marketo_form_name,
    marketo_form_fill.primary_attribute_value_id  AS marketo_form_id,
    marketo_form_fill.form_fields,
    marketo_form_fill.webpage_id                  AS marketo_webpage_id,
    marketo_form_fill.query_parameters            AS marketo_query_parameters,
    marketo_form_fill.referrer_url                AS marketo_referrer_url,
    marketo_form_fill.client_ip_address_hash      AS marketo_form_client_ip_address_hash,
    marketo_form_fill.campaign_id                 AS marketo_campaign_id,
    'Form - Self-Managed Trial' AS activity_type,
    TRUE                        AS in_current_scoring_model,
    CASE
      WHEN prep_crm_person.email_domain_type NOT IN ('Business email domain', 'Personal email domain')
        AND dim_marketing_contact.has_namespace_setup_for_company_use = FALSE
        THEN 'Trial - Default'
      WHEN prep_crm_person.email_domain_type = 'Business email domain'
        OR (
          dim_marketing_contact.has_namespace_setup_for_company_use = TRUE
          AND prep_crm_person.email_domain_type != 'Personal email domain'
          )
        THEN 'Trial - Business'
      WHEN prep_crm_person.email_domain_type = 'Personal email domain'
        THEN 'Trial - Personal'
    END                                           AS scored_action,
    CASE
      WHEN scored_action = 'Trial - Default'
        THEN 'Self-Managed - Default' 
      WHEN scored_action = 'Trial - Business'
        THEN 'Self-Managed - Business' 
      WHEN scored_action = 'Trial - Personal'
        THEN 'Self-Managed - Personal'
      ELSE NULL
    END                                           AS trial_type
  FROM marketo_form_fill
  LEFT JOIN prep_crm_person
    ON marketo_form_fill.lead_id = prep_crm_person.marketo_lead_id
  LEFT JOIN dim_marketing_contact
    ON prep_crm_person.sfdc_record_id = dim_marketing_contact.sfdc_record_id
  WHERE form_ids_type = 'Trial'

),

filled_out_form_general AS (

  SELECT
    marketo_activity_fill_out_form_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_form_name,
    primary_attribute_value_id AS marketo_form_id,
    form_fields,
    webpage_id                 AS marketo_webpage_id,
    query_parameters           AS marketo_query_parameters,
    referrer_url               AS marketo_referrer_url,
    client_ip_address_hash     AS marketo_form_client_ip_address_hash,
    campaign_id                AS marketo_campaign_id,
    'Form - General'           AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM marketo_form_fill
  WHERE form_ids_type = 'General'

),

fill_out_li_form AS (

  SELECT
    marketo_activity_fill_out_linkedin_lead_gen_form_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS linkedin_form_name,
    primary_attribute_value_id AS marketo_form_id,
    campaign_id                AS marketo_campaign_id,
    'LI Form'                  AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_fill_out_linkedin_lead_gen_form_source') }}

),

marketo_visit_web_page AS (

  SELECT {{ hash_sensitive_columns('marketo_activity_visit_webpage_source') }}
  FROM {{ ref('marketo_activity_visit_webpage_source') }}

),

visit_key_page AS (

  SELECT
    marketo_activity_visit_webpage_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS webpage,
    client_ip_address_hash     AS marketo_form_client_ip_address_hash,
    activity_type_id,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value_id AS marketo_webpage_id,
    webpage_url,
    search_engine,
    query_parameters           AS marketo_query_parameters,
    search_query,
    'Key Page'                 AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Visits Key Webpage'       AS scored_action
  FROM marketo_visit_web_page
  WHERE (
    primary_attribute_value LIKE '%/pricing%'
    OR primary_attribute_value LIKE '%/get-started%'
    OR primary_attribute_value LIKE '%/install%'
    OR primary_attribute_value LIKE '%/free-trial%'
    OR primary_attribute_value LIKE '%/livestream%'
  )
  AND primary_attribute_value NOT LIKE '%docs.gitlab.com%'

),

visit_low_page AS (

  SELECT
    marketo_activity_visit_webpage_id
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS webpage,
    client_ip_address_hash     AS marketo_form_client_ip_address_hash,
    activity_type_id,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value_id AS marketo_webpage_id,
    webpage_url,
    search_engine,
    query_parameters           AS marketo_query_parameters,
    search_query,
    'Low Page'                 AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Web: Visits Low Value'    AS scored_action
  FROM marketo_visit_web_page
  WHERE primary_attribute_value LIKE '%/jobs%'
    AND primary_attribute_value NOT LIKE '%docs.gitlab.com%'

),

visit_multi_pages_base AS (

  SELECT
    marketo_activity_visit_webpage_id                 AS dim_marketo_activity_id,
    lead_id                                           AS dim_marketo_person_id,
    activity_date::TIMESTAMP                          AS activity_datetime,
    activity_date::DATE                               AS activity_date,
    COUNT(DISTINCT marketo_activity_visit_webpage_id) AS visits
  FROM marketo_visit_web_page
  {{ dbt_utils.group_by(n=4) }}

),

email_bounced AS (

  SELECT
    marketo_activity_email_bounced_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id                AS marketo_campaign_id,
    campaign_run_id            AS marketo_campaign_run_id,
    category                   AS marketo_email_category,
    details                    AS marketo_email_bounce_reason,
    subcategory                AS marketo_email_subcategory,
    step_id                    AS marketo_email_program_step_id,
    test_variant               AS marketo_email_test_variant,
    'Email: Bounced'           AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Email: Bounce'            AS scored_action
  FROM {{ ref('marketo_activity_email_bounced_source') }}

),

email_unsubscribe AS (

  SELECT
    marketo_activity_unsubscribe_email_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id                AS marketo_campaign_id,
    campaign_run_id            AS marketo_campaign_run_id,
    webform_id                 AS marketo_form_id,
    form_fields,
    webpage_id                 AS marketo_webpage_id,
    query_parameters           AS marketo_query_parameters,
    referrer_url               AS marketo_referrer_url,
    test_variant               AS marketo_email_test_variant,
    'Email: Unsubscribe'       AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'Email: Unsubscribed'      AS scored_action
  FROM {{ ref('marketo_activity_unsubscribe_email_source') }}

),

lead_score_decay AS (

  SELECT
    marketo_activity_change_score_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    reason                     AS marketo_score_decay_reason,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value    AS marketo_score_decay_type,
    'Score Decay'              AS activity_type,
    TRUE                       AS in_current_scoring_model,
    'No activity in 30 days'   AS scored_action,
    SUM(new_value - old_value) AS score_change
  FROM marketo_activity_change_score_source
  WHERE change_value LIKE '%Decay%'
  {{ dbt_utils.group_by(n=9) }}

),

six_qa_boost AS (

  SELECT
    marketo_activity_change_score_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value    AS changed_score_field,
    change_value               AS scoring_rule,
    '6QA Boost'                AS activity_type,
    TRUE                       AS in_current_scoring_model,
    '6QA Boost'                AS scored_action
  FROM marketo_activity_change_score_source
  WHERE reason = 'Changed by Smart Campaign OP-Scoring_2020.6QA Identified action Change Score'

),

saas_trial AS (

  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date,
    campaign_name,
    campaign_id,
    campaign_type,
    campaign_member_status,
    marketo_campaign_id,
    activity_type,
    TRUE               AS in_current_scoring_model,
    CASE
      WHEN prep_crm_person.email_domain_type NOT IN ('Business email domain', 'Personal email domain')
        AND dim_marketing_contact.has_namespace_setup_for_company_use = FALSE
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%saas%'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%trial%'
        THEN 'Trial - Default'
      WHEN (prep_crm_person.email_domain_type = 'Business email domain'
        OR (
          dim_marketing_contact.has_namespace_setup_for_company_use = TRUE
          AND prep_crm_person.email_domain_type != 'Personal email domain'
        ))
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%saas%'
        AND LOWER(combined_campaigns_with_activity_type.campaign_name) LIKE '%trial%'
        THEN 'Trial - Business'
      WHEN prep_crm_person.email_domain_type = 'Personal email domain'
        THEN 'Trial - Personal'
    END                AS scored_action,
    CASE
      WHEN scored_action = 'Trial - Default'
        THEN 'SaaS - Default' 
      WHEN scored_action = 'Trial - Business'
        THEN 'SaaS - Business' 
      WHEN scored_action = 'Trial - Personal'
        THEN 'SaaS - Personal'
      ELSE NULL
    END                AS trial_type
  FROM combined_campaigns_with_activity_type
  LEFT JOIN prep_crm_person
    ON combined_campaigns_with_activity_type.dim_marketo_person_id = prep_crm_person.marketo_lead_id
  LEFT JOIN dim_marketing_contact
    ON prep_crm_person.sfdc_record_id = dim_marketing_contact.sfdc_record_id
  WHERE activity_type = 'Form - SaaS Trial'

),

add_to_nurture AS (

  SELECT
    marketo_activity_add_to_nurture_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value    AS marketo_nurture,
    primary_attribute_value_id AS marketo_nurture_id,
    track_id                   AS marketo_nurture_track_id,
    'Add to Nurture'           AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_add_to_nurture_source') }}

),

change_nurture_track AS (

  SELECT
    marketo_activity_change_nurture_track_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    campaign_id                AS marketo_campaign_id,
    primary_attribute_value    AS marketo_nurture,
    primary_attribute_value_id AS marketo_nurture_id,
    previous_track_id          AS marketo_nurture_previous_track_id,
    new_track_id               AS marketo_nurture_new_track_id,
    'Change Nurture Track'     AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_change_nurture_track_source') }}

),

change_score AS (

  SELECT
    marketo_activity_change_score_id 
                             AS dim_marketo_activity_id,
    lead_id                  AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE      AS activity_date,
    primary_attribute_value  AS changed_score_field,
    CASE
      WHEN change_value IS NULL 
        THEN 'Do Nothing'
      ELSE change_value
    END                      AS scoring_rule,
    old_value                AS old_score,
    new_value                AS new_score,
    'Score Change'           AS activity_type,
    FALSE                    AS in_current_scoring_model,
    NULL                     AS scored_action
  FROM marketo_activity_change_score_source

),

click_link AS (

  SELECT
    marketo_activity_click_link_id 
                             AS dim_marketo_activity_id,
    lead_id                  AS dim_marketo_person_id,
    activity_date::TIMESTAMP AS activity_datetime,
    activity_date::DATE      AS activity_date,
    campaign_id              AS marketo_campaign_id,
    primary_attribute_value  AS clicked_url,
    webpage_id               AS marketo_webpage_id,
    query_parameters         AS marketo_query_parameters,
    referrer_url             AS marketo_referrer_url,
    'Click Email Link'       AS activity_type,
    FALSE                    AS in_current_scoring_model,
    NULL                     AS scored_action
  FROM {{ ref('marketo_activity_click_link_source') }}

),

email_delivered AS (

  SELECT
    marketo_activity_email_delivered_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id                AS marketo_campaign_id,
    step_id                    AS marketo_email_program_step_id,
    test_variant               AS marketo_email_test_variant,
    'Email Delivered'          AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_email_delivered_source') }}

),

email_opened AS (

  SELECT
    marketo_activity_open_email_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id                AS marketo_campaign_id,
    test_variant               AS marketo_email_test_variant,
    is_bot_activity,
    'Email Opened'             AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_open_email_source') }}

),

push_lead_to_marketo AS (

  SELECT
    marketo_activity_push_lead_to_marketo_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS push_lead_to_marketo_source,
    primary_attribute_value_id AS push_lead_to_marketo_source_id,
    campaign_id                AS marketo_campaign_id,
    'Push Lead to Marketo'     AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_push_lead_to_marketo_source') }}

),

email_sent AS (

  SELECT
    marketo_activity_send_email_id 
                               AS dim_marketo_activity_id,
    lead_id                    AS dim_marketo_person_id,
    activity_date::TIMESTAMP   AS activity_datetime,
    activity_date::DATE        AS activity_date,
    primary_attribute_value    AS marketo_email_name,
    primary_attribute_value_id AS marketo_email_id,
    campaign_id                AS marketo_campaign_id,
    test_variant               AS marketo_email_test_variant,
    'Email Sent'               AS activity_type,
    FALSE                      AS in_current_scoring_model,
    NULL                       AS scored_action
  FROM {{ ref('marketo_activity_send_email_source') }}

),

push_lead_to_sfdc AS (

  SELECT
    marketo_activity_sync_lead_to_sfdc_id 
                              AS dim_marketo_activity_id,
    lead_id                   AS dim_marketo_person_id,
    activity_date::TIMESTAMP  AS activity_datetime,
    activity_date::DATE       AS activity_date,
    campaign_id               AS marketo_campaign_id,
    'Sync Lead to Salesforce' AS activity_type,
    FALSE                     AS in_current_scoring_model,
    NULL                      AS scored_action
  FROM {{ ref('marketo_activity_sync_lead_to_sfdc_source') }}

),

activities_date_prep AS (

  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM combined_campaigns_with_activity_type
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM inbound_forms_high
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM subscription
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM fill_out_li_form
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM visit_key_page
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM visit_low_page
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM email_bounced
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM email_unsubscribe
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM lead_score_decay
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM six_qa_boost
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM self_managed_trial
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM saas_trial
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM filled_out_form_general
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM add_to_nurture
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM change_nurture_track
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM change_score
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM click_link
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM email_delivered
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM email_sent
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM email_opened
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM push_lead_to_marketo
  UNION
  SELECT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM push_lead_to_sfdc

),

activites_date_final AS (

  SELECT DISTINCT
    dim_marketo_activity_id,
    dim_marketo_person_id,
    activity_datetime,
    activity_date
  FROM activities_date_prep

),

activities_final AS (

  SELECT
    activites_date_final.dim_marketo_person_id,
    prep_crm_person.dim_crm_person_id,
    prep_crm_person.sfdc_record_id,
    activites_date_final.dim_marketo_activity_id,
    activites_date_final.activity_datetime,
    activites_date_final.activity_date,
    COALESCE(combined_campaigns_with_activity_type.campaign_name, saas_trial.campaign_name)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS campaign_name,
    COALESCE(combined_campaigns_with_activity_type.campaign_id, saas_trial.campaign_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                           AS campaign_id,
    COALESCE(combined_campaigns_with_activity_type.marketo_campaign_id, inbound_forms_high.marketo_campaign_id, subscription.marketo_campaign_id, fill_out_li_form.marketo_campaign_id, visit_key_page.marketo_campaign_id, visit_low_page.marketo_campaign_id, six_qa_boost.marketo_campaign_id, email_bounced.marketo_campaign_id, email_unsubscribe.marketo_campaign_id, lead_score_decay.marketo_campaign_id, self_managed_trial.marketo_campaign_id, filled_out_form_general.marketo_campaign_id, change_nurture_track.marketo_campaign_id, click_link.marketo_campaign_id, email_delivered.marketo_campaign_id, email_opened.marketo_campaign_id, push_lead_to_marketo.marketo_campaign_id, email_sent.marketo_campaign_id, push_lead_to_sfdc.marketo_campaign_id)                                                                                                                                                            AS marketo_campaign_id,
    COALESCE(combined_campaigns_with_activity_type.campaign_type, saas_trial.campaign_type)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS campaign_type,
    COALESCE(combined_campaigns_with_activity_type.campaign_member_status, saas_trial.campaign_member_status)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AS campaign_member_status,
    COALESCE(inbound_forms_high.marketo_form_name, subscription.marketo_form_name, self_managed_trial.marketo_form_name, filled_out_form_general.marketo_form_name)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               AS marketo_form_name,
    COALESCE(inbound_forms_high.marketo_form_id, subscription.marketo_form_id, fill_out_li_form.marketo_form_id, email_unsubscribe.marketo_form_id, self_managed_trial.marketo_form_id, filled_out_form_general.marketo_form_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                  AS marketo_form_id,
    COALESCE(inbound_forms_high.form_fields, subscription.form_fields, email_unsubscribe.form_fields, self_managed_trial.form_fields, filled_out_form_general.form_fields)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                        AS form_fields,
    COALESCE(inbound_forms_high.marketo_webpage_id, subscription.marketo_webpage_id, self_managed_trial.marketo_webpage_id, filled_out_form_general.marketo_webpage_id, click_link.marketo_webpage_id, visit_key_page.marketo_webpage_id, visit_low_page.marketo_webpage_id, email_unsubscribe.marketo_webpage_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AS marketo_webpage_id,
    COALESCE(inbound_forms_high.marketo_query_parameters, visit_key_page.marketo_query_parameters, visit_low_page.marketo_query_parameters, email_unsubscribe.marketo_query_parameters, subscription.marketo_query_parameters, self_managed_trial.marketo_query_parameters, filled_out_form_general.marketo_query_parameters, click_link.marketo_query_parameters)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                AS marketo_query_parameters,
    COALESCE(inbound_forms_high.marketo_referrer_url, subscription.marketo_referrer_url, email_unsubscribe.marketo_referrer_url, self_managed_trial.marketo_referrer_url, filled_out_form_general.marketo_referrer_url, click_link.marketo_referrer_url)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                          AS marketo_referrer_url,
    COALESCE(inbound_forms_high.marketo_form_client_ip_address_hash, subscription.marketo_form_client_ip_address_hash, visit_key_page.marketo_form_client_ip_address_hash, visit_low_page.marketo_form_client_ip_address_hash, self_managed_trial.marketo_form_client_ip_address_hash, filled_out_form_general.marketo_form_client_ip_address_hash)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               AS marketo_form_client_ip_address_hash,

    fill_out_li_form.linkedin_form_name,
    COALESCE(visit_key_page.webpage, visit_low_page.webpage)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               AS webpage,
    COALESCE(visit_key_page.webpage_url, visit_low_page.webpage_url)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS webpage_url,
    COALESCE(visit_key_page.search_engine, visit_low_page.search_engine)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS search_engine,
    COALESCE(visit_key_page.search_query, visit_low_page.search_query)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                     AS search_query,
    COALESCE(visit_key_page.activity_type_id, visit_low_page.activity_type_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AS web_activity_type_id,
    COALESCE(change_nurture_track.marketo_nurture, add_to_nurture.marketo_nurture)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         AS marketo_nurture,
    COALESCE(change_nurture_track.marketo_nurture_id, add_to_nurture.marketo_nurture_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS marketo_nurture_id,
    change_nurture_track.marketo_nurture_previous_track_id,
    change_nurture_track.marketo_nurture_new_track_id,
    COALESCE(email_bounced.marketo_email_name, email_unsubscribe.marketo_email_name, email_delivered.marketo_email_name, email_opened.marketo_email_name, email_sent.marketo_email_name)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS marketo_email_name,
    COALESCE(email_bounced.marketo_email_id, email_unsubscribe.marketo_email_id, email_delivered.marketo_email_id, email_opened.marketo_email_id, email_sent.marketo_email_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AS marketo_email_id,
    COALESCE(email_bounced.marketo_campaign_run_id, email_unsubscribe.marketo_campaign_run_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                             AS marketo_campaign_run_id,
    email_bounced.marketo_email_category,
    email_bounced.marketo_email_bounce_reason,
    email_bounced.marketo_email_subcategory,
    COALESCE(email_bounced.marketo_email_program_step_id, email_delivered.marketo_email_program_step_id)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                   AS marketo_email_program_step_id,
    COALESCE(email_bounced.marketo_email_test_variant, email_delivered.marketo_email_test_variant, email_opened.marketo_email_test_variant, email_opened.marketo_email_test_variant)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                       AS marketo_email_test_variant,
    COALESCE(six_qa_boost.changed_score_field,change_score.changed_score_field) AS changed_score_field,
    COALESCE(six_qa_boost.scoring_rule,change_score.scoring_rule) AS scoring_rule,
    click_link.clicked_url,
    push_lead_to_marketo.push_lead_to_marketo_source,
    push_lead_to_marketo.push_lead_to_marketo_source_id,
    lead_score_decay.marketo_score_decay_reason,
    lead_score_decay.marketo_score_decay_type,
    COALESCE(self_managed_trial.trial_type, saas_trial.trial_type)                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                         AS trial_type,
    COALESCE(combined_campaigns_with_activity_type.activity_type, inbound_forms_high.activity_type, subscription.activity_type, six_qa_boost.activity_type, fill_out_li_form.activity_type, visit_key_page.activity_type, visit_low_page.activity_type, email_bounced.activity_type, email_unsubscribe.activity_type, lead_score_decay.activity_type, self_managed_trial.activity_type, saas_trial.activity_type, filled_out_form_general.activity_type, change_nurture_track.activity_type, change_score.activity_type, click_link.activity_type, email_delivered.activity_type, email_opened.activity_type, push_lead_to_marketo.activity_type, email_sent.activity_type, push_lead_to_sfdc.activity_type, add_to_nurture.activity_type)                                                                                                                                                                                               AS activity_type,
    COALESCE(combined_campaigns_with_activity_type.in_current_scoring_model,inbound_forms_high.in_current_scoring_model, subscription.in_current_scoring_model, fill_out_li_form.in_current_scoring_model, six_qa_boost.in_current_scoring_model, visit_key_page.in_current_scoring_model, visit_low_page.in_current_scoring_model, email_bounced.in_current_scoring_model, email_unsubscribe.in_current_scoring_model, lead_score_decay.in_current_scoring_model, self_managed_trial.in_current_scoring_model, saas_trial.in_current_scoring_model, filled_out_form_general.in_current_scoring_model, change_nurture_track.in_current_scoring_model, change_score.in_current_scoring_model, click_link.in_current_scoring_model, email_delivered.in_current_scoring_model, email_opened.in_current_scoring_model, push_lead_to_marketo.in_current_scoring_model, email_sent.in_current_scoring_model, push_lead_to_sfdc.in_current_scoring_model, add_to_nurture.in_current_scoring_model)             AS in_current_scoring_model,
    COALESCE(inbound_forms_high.scored_action, subscription.scored_action, fill_out_li_form.scored_action, visit_key_page.scored_action, six_qa_boost.scored_action, visit_low_page.scored_action, email_bounced.scored_action, email_unsubscribe.scored_action, lead_score_decay.scored_action, self_managed_trial.scored_action, saas_trial.scored_action, filled_out_form_general.scored_action, change_nurture_track.scored_action, change_score.scored_action, click_link.scored_action, email_delivered.scored_action, email_opened.scored_action, push_lead_to_marketo.scored_action, email_sent.scored_action, push_lead_to_sfdc.scored_action, add_to_nurture.scored_action)                                                                                                                                                                                                                                                    AS scored_action,
    change_score.old_score AS score_change_original_value,
    change_score.new_score AS score_change_new_value,
    SUM(score_change_new_value-score_change_original_value) AS score_change_net_value
  FROM activites_date_final
  LEFT JOIN combined_campaigns_with_activity_type
    ON activites_date_final.dim_marketo_person_id = combined_campaigns_with_activity_type.dim_marketo_person_id
      AND activites_date_final.activity_datetime = combined_campaigns_with_activity_type.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = combined_campaigns_with_activity_type.dim_marketo_activity_id
  LEFT JOIN inbound_forms_high
    ON activites_date_final.dim_marketo_person_id = inbound_forms_high.dim_marketo_person_id
      AND activites_date_final.activity_datetime = inbound_forms_high.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = inbound_forms_high.dim_marketo_activity_id
  LEFT JOIN subscription
    ON activites_date_final.dim_marketo_person_id = subscription.dim_marketo_person_id
      AND activites_date_final.activity_datetime = subscription.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = subscription.dim_marketo_activity_id
  LEFT JOIN fill_out_li_form
    ON activites_date_final.dim_marketo_person_id = fill_out_li_form.dim_marketo_person_id
      AND activites_date_final.activity_datetime = fill_out_li_form.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = fill_out_li_form.dim_marketo_activity_id
  LEFT JOIN visit_key_page
    ON activites_date_final.dim_marketo_person_id = visit_key_page.dim_marketo_person_id
      AND activites_date_final.activity_datetime = visit_key_page.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = visit_key_page.dim_marketo_activity_id
  LEFT JOIN visit_low_page
    ON activites_date_final.dim_marketo_person_id = visit_low_page.dim_marketo_person_id
      AND activites_date_final.activity_datetime = visit_low_page.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = visit_low_page.dim_marketo_activity_id
  LEFT JOIN email_bounced
    ON activites_date_final.dim_marketo_person_id = email_bounced.dim_marketo_person_id
      AND activites_date_final.activity_datetime = email_bounced.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = email_bounced.dim_marketo_activity_id
  LEFT JOIN email_unsubscribe
    ON activites_date_final.dim_marketo_person_id = email_unsubscribe.dim_marketo_person_id
      AND activites_date_final.activity_datetime = email_unsubscribe.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = email_unsubscribe.dim_marketo_activity_id
  LEFT JOIN lead_score_decay
    ON activites_date_final.dim_marketo_person_id = lead_score_decay.dim_marketo_person_id
      AND activites_date_final.activity_datetime = lead_score_decay.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = lead_score_decay.dim_marketo_activity_id
  LEFT JOIN six_qa_boost
    ON activites_date_final.dim_marketo_person_id = six_qa_boost.dim_marketo_person_id
      AND activites_date_final.activity_datetime = six_qa_boost.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = six_qa_boost.dim_marketo_activity_id
  LEFT JOIN self_managed_trial
    ON activites_date_final.dim_marketo_person_id = self_managed_trial.dim_marketo_person_id
      AND activites_date_final.activity_datetime = self_managed_trial.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = self_managed_trial.dim_marketo_activity_id
  LEFT JOIN saas_trial
    ON activites_date_final.dim_marketo_person_id = saas_trial.dim_marketo_person_id
      AND activites_date_final.activity_datetime = saas_trial.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = saas_trial.dim_marketo_activity_id
  LEFT JOIN filled_out_form_general
    ON activites_date_final.dim_marketo_person_id = filled_out_form_general.dim_marketo_person_id
      AND activites_date_final.activity_datetime = filled_out_form_general.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = filled_out_form_general.dim_marketo_activity_id
  LEFT JOIN add_to_nurture
    ON activites_date_final.dim_marketo_person_id = add_to_nurture.dim_marketo_person_id
      AND activites_date_final.activity_datetime = add_to_nurture.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = add_to_nurture.dim_marketo_activity_id
  LEFT JOIN change_nurture_track
    ON activites_date_final.dim_marketo_person_id = change_nurture_track.dim_marketo_person_id
      AND activites_date_final.activity_datetime = change_nurture_track.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = change_nurture_track.dim_marketo_activity_id
  LEFT JOIN change_score
    ON activites_date_final.dim_marketo_person_id = change_score.dim_marketo_person_id
      AND activites_date_final.activity_datetime = change_score.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = change_score.dim_marketo_activity_id
  LEFT JOIN click_link
    ON activites_date_final.dim_marketo_person_id = click_link.dim_marketo_person_id
      AND activites_date_final.activity_datetime = click_link.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = click_link.dim_marketo_activity_id
  LEFT JOIN email_delivered
    ON activites_date_final.dim_marketo_person_id = email_delivered.dim_marketo_person_id
      AND activites_date_final.activity_datetime = email_delivered.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = email_delivered.dim_marketo_activity_id
  LEFT JOIN email_opened
    ON activites_date_final.dim_marketo_person_id = email_opened.dim_marketo_person_id
      AND activites_date_final.activity_datetime = email_opened.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = email_opened.dim_marketo_activity_id
  LEFT JOIN push_lead_to_marketo
    ON activites_date_final.dim_marketo_person_id = push_lead_to_marketo.dim_marketo_person_id
      AND activites_date_final.activity_datetime = push_lead_to_marketo.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = push_lead_to_marketo.dim_marketo_activity_id
  LEFT JOIN email_sent
    ON activites_date_final.dim_marketo_person_id = email_sent.dim_marketo_person_id
      AND activites_date_final.activity_datetime = email_sent.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = email_sent.dim_marketo_activity_id
  LEFT JOIN push_lead_to_sfdc
    ON activites_date_final.dim_marketo_person_id = push_lead_to_sfdc.dim_marketo_person_id
      AND activites_date_final.activity_datetime = push_lead_to_sfdc.activity_datetime
      AND activites_date_final.dim_marketo_activity_id = push_lead_to_sfdc.dim_marketo_activity_id
  LEFT JOIN prep_crm_person
    ON activites_date_final.dim_marketo_person_id = prep_crm_person.marketo_lead_id
  {{dbt_utils.group_by(n=49)}}

),

final AS (

  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['dim_marketo_person_id','dim_marketo_activity_id','activity_datetime','activity_type','scored_action','form_fields','campaign_id','marketo_campaign_id','marketo_email_id','marketo_webpage_id','campaign_type']) }} AS marketo_activity_sk,
    activities_final.*,
    CASE
      WHEN activity_type IN ('Click Email Link','Cohort: Organic Engaged','Conference','Conference: Visited Booth','Content Syndication','Email: Clicked Link','Email: Opened','Event Follow Up Requested','Event: Registered','Exec Roundtable','Form - General','Form - SaaS Trial','Form - Self-Managed Trial','Form - Subscription','Gated Content: Analyst Report','Gated Content: Other','Inbound Request: DNI','Inbound Request: High','Inbound Request: Medium','Inbound Request: Offline - PQL','Key Page','LI Form','Low Page','Owned Event','Paid Social','PF: Content','PF: Fast Moving Buyer boost','Self Service Virtual Event','Speaking Session','Sponsored Webcast','Survey: High','Survey: Low','Vendor Meeting','Web Chat (Drift)','Web Chat (Qualified)','Webcast','Webcast: Tech Demo','Workshop')
        THEN TRUE
        ELSE FALSE
    END AS is_person_activity
  FROM activities_final
  WHERE dim_marketo_person_id IS NOT NULL
    AND marketo_activity_sk IS NOT NULL

)

SELECT *
FROM final

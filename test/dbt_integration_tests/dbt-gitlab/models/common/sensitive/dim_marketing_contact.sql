WITH sfdc_lead AS (

  SELECT *
  FROM {{ ref('sfdc_lead_source') }}

),

sfdc_contact AS (

  SELECT *
  FROM {{ ref('sfdc_contact_source') }}

),

sfdc_account AS (

  SELECT *
  FROM {{ ref('sfdc_account_source') }}

),

marketo AS (

  SELECT *
  FROM {{ ref('marketo_lead_source') }}

),

crm_account AS (

  SELECT *
  FROM {{ ref('dim_crm_account') }}

),

sales_segment AS (

  SELECT *
  FROM {{ ref('prep_sales_segment') }}

),

crm_person AS (

  SELECT *
  FROM {{ ref('prep_crm_person') }}

),

gitlab_users AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_users_source') }}

),

customer_db_source AS (

  SELECT *
  FROM {{ ref('customers_db_customers_source') }}

),

zuora_contact_source AS (

  SELECT *
  FROM {{ ref('zuora_contact_source') }}

),

zuora_account_source AS (

  SELECT *
  FROM {{ ref('zuora_account_source') }}

), 

gitlab_dotcom_user_details_source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_user_details_source') }}

),

dnc_list AS (

  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY CASE WHEN result IN ('undeliverable', 'do_not_send') THEN 2 ELSE 1 END DESC) AS record_number
  FROM {{ ref('driveload_marketing_dnc_list_source') }}
  QUALIFY record_number = 1

),

sfdc AS (

  SELECT
    crm_person.sfdc_record_id,
    crm_person.dim_crm_account_id,
    CASE WHEN crm_person.sfdc_record_type = 'contact' THEN sfdc_contact.contact_email ELSE sfdc_lead.lead_email END AS email_address,
    crm_person.dim_crm_person_id                                                                                    AS crm_person_id,
    crm_person.sfdc_record_type                                                                                     AS sfdc_lead_contact,
    CASE
      WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.contact_first_name
      ELSE sfdc_lead.lead_first_name
    END                                                                                                             AS first_name,
    CASE
      WHEN sfdc_lead_contact = 'contact' AND sfdc_contact.contact_last_name = '[[unknown]]' THEN NULL
      WHEN sfdc_lead_contact = 'contact' AND sfdc_contact.contact_last_name != '[[unknown]]' THEN sfdc_contact.contact_last_name
      WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.lead_last_name = '[[unknown]]' THEN NULL
      WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.lead_last_name != '[[unknown]]' THEN sfdc_lead.lead_last_name
    END                                                                                                             AS last_name,
    CASE
      WHEN sfdc_lead_contact = 'contact' AND sfdc_account.account_name = '[[unknown]]' THEN NULL
      WHEN sfdc_lead_contact = 'contact' AND sfdc_account.account_name != '[[unknown]]' THEN sfdc_account.account_name
      WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.company = '[[unknown]]' THEN NULL
      WHEN sfdc_lead_contact = 'lead' AND sfdc_lead.company != '[[unknown]]' THEN sfdc_lead.company
    END                                                                                                             AS company_name,
    crm_person.title                                                                                                AS job_title,
    crm_person.it_job_title_hierarchy,
    crm_person.contact_role,
    crm_account.parent_crm_account_sales_segment                                                                    AS parent_crm_account_sales_segment,
    crm_account.parent_crm_account_region                                                                           AS parent_crm_account_region,
    crm_person.account_demographics_geo                                                                             AS crm_person_region,
    CASE
      WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.mailing_country
      ELSE sfdc_lead.country
    END                                                                                                             AS country,
    sfdc_contact.mobile_phone,
    CASE
      WHEN sfdc_lead_contact = 'contact' THEN sfdc_contact.created_date
      ELSE sfdc_lead.created_date
    END                                                                                                             AS sfdc_created_date,
    crm_person.has_opted_out_email                                                                                  AS opted_out_salesforce,
    (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY sfdc_created_date DESC))                                AS record_number

  FROM crm_person
  LEFT JOIN sfdc_contact
    ON crm_person.sfdc_record_id = sfdc_contact.contact_id
  LEFT JOIN sfdc_lead
    ON sfdc_lead.lead_id = sfdc_record_id
  LEFT JOIN sfdc_account
    ON sfdc_contact.account_id = sfdc_account.account_id
  LEFT JOIN crm_account
    ON crm_person.dim_crm_account_id = crm_account.dim_crm_account_id
  WHERE email_address IS NOT NULL
    AND email_address != ''
  QUALIFY record_number = 1

),

marketo_lead AS (

  SELECT
    marketo_lead_id,
    email                                                             AS email_address,
    first_name,
    last_name,
    IFF(company_name = '[[unknown]]', NULL, company_name)             AS company_name,
    job_title,
    it_job_title_hierarchy,
    country,
    mobile_phone,
    is_lead_inactive,
    is_contact_inactive,
    IFF(sales_segmentation = 'Unknown', NULL, sales_segmentation)     AS sales_segmentation,
    is_email_bounced                                                  AS is_marketo_email_bounced,
    email_bounced_date                                                AS marketo_email_bounced_date,
    is_unsubscribed                                                   AS is_marketo_unsubscribed,
    COALESCE(is_marketo_unsubscribed, FALSE)                          AS has_marketo_unsubscribed,
    is_opt_in                                                         AS is_marketo_opted_in,
    compliance_segment_value                                          AS marketo_compliance_segment_value,
    is_pql_marketo,
    is_paid_tier_marketo,
    is_ptpt_contact_marketo,
    is_ptp_contact_marketo,
    is_impacted_by_user_limit_marketo,
    is_currently_in_trial_marketo,
    trial_start_date_marketo,
    trial_end_date_marketo,
    (ROW_NUMBER() OVER (PARTITION BY email ORDER BY updated_at DESC)) AS record_number

  FROM marketo
  WHERE email IS NOT NULL
    OR email != ''
  QUALIFY record_number = 1

),

gitlab_dotcom AS (

  SELECT
    email                                                                       AS email_address,
    gitlab_users.user_id                                                        AS user_id,
    SPLIT_PART(users_name, ' ', 1)                                              AS first_name,
    SPLIT_PART(users_name, ' ', 2)                                              AS last_name,
    user_name                                                                   AS user_name,
    organization                                                                AS company_name,
    role                                                                        AS job_title,
    it_job_title_hierarchy,
    created_at                                                                  AS created_date,
    confirmed_at                                                                AS confirmed_date,
    state                                                                       AS active_state,
    last_sign_in_at                                                             AS last_login_date,
    gitlab_dotcom_user_details_source.initial_email_opt_in_value                AS email_opted_in,
    (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY created_date DESC)) AS record_number
  FROM gitlab_users
  LEFT JOIN gitlab_dotcom_user_details_source
    ON gitlab_users.user_id=gitlab_dotcom_user_details_source.user_id
  WHERE email_address IS NOT NULL
    AND email_address != ''
    AND active_state = 'active'
  QUALIFY record_number = 1

),

customer_db AS (

  SELECT
    customer_email                                                              AS email_address,
    customer_id                                                                 AS customer_id,
    customer_first_name                                                         AS first_name,
    customer_last_name                                                          AS last_name,
    company                                                                     AS company_name,
    country                                                                     AS country,
    customer_created_at                                                         AS created_date,
    confirmed_at                                                                AS confirmed_date,
    company_size                                                                AS market_segment,
    last_sign_in_at                                                             AS last_login_date,
    (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY created_date DESC)) AS record_number
  FROM customer_db_source
  WHERE email_address IS NOT NULL
    AND email_address != ''
    AND confirmed_at IS NOT NULL
  QUALIFY record_number = 1

),

zuora AS (

  SELECT
    zuora_contact_source.work_email                                                                  AS email_address,
    zuora_contact_source.contact_id                                                                  AS contact_id,
    zuora_contact_source.first_name                                                                  AS first_name,
    zuora_contact_source.last_name                                                                   AS last_name,
    zuora_account_source.account_name                                                                AS company_name,
    zuora_contact_source.country                                                                     AS country,
    zuora_contact_source.created_date                                                                AS created_date,
    CASE
      WHEN zuora_contact_source.is_deleted = TRUE THEN 'Inactive'
      ELSE 'Active'
    END                                                                                              AS active_state,
    (ROW_NUMBER() OVER (PARTITION BY email_address ORDER BY zuora_contact_source.created_date DESC)) AS record_number
  FROM zuora_contact_source
  INNER JOIN zuora_account_source
    ON zuora_contact_source.account_id = zuora_account_source.account_id
  WHERE email_address IS NOT NULL
    AND email_address != ''
    AND zuora_contact_source.is_deleted = FALSE
  QUALIFY record_number = 1

),

emails AS (

  SELECT email_address
  FROM sfdc

  UNION

  SELECT email_address
  FROM gitlab_dotcom

  UNION

  SELECT email_address
  FROM customer_db

  UNION

  SELECT email_address
  FROM zuora

  UNION

  SELECT email_address
  FROM marketo_lead

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['emails.email_address']) }}                                AS dim_marketing_contact_id,
    emails.email_address,
    SPLIT_PART(emails.email_address, '@', 2)                                                                                         AS email_domain,
    COALESCE(zuora.first_name, marketo_lead.first_name, sfdc.first_name, customer_db.first_name, gitlab_dotcom.first_name)
      AS first_name,
    COALESCE(zuora.last_name, marketo_lead.last_name, sfdc.last_name, customer_db.last_name, gitlab_dotcom.last_name)                AS last_name,
    gitlab_dotcom.user_name                                                                                                          AS gitlab_user_name,
    COALESCE(zuora.company_name, marketo_lead.company_name, sfdc.company_name, customer_db.company_name, gitlab_dotcom.company_name)
      AS company_name,
    COALESCE(marketo_lead.job_title, sfdc.job_title, gitlab_dotcom.job_title)                                                        AS job_title,
    CASE
      WHEN marketo_lead.job_title IS NOT NULL THEN marketo_lead.it_job_title_hierarchy
      WHEN sfdc.job_title IS NOT NULL THEN sfdc.it_job_title_hierarchy
      ELSE gitlab_dotcom.it_job_title_hierarchy
    END                                                                                                                              AS it_job_title_hierarchy,
    COALESCE(zuora.country, marketo_lead.country, sfdc.country, customer_db.country)                                                 AS country,
    sfdc.parent_crm_account_sales_segment                                                                                            AS sfdc_parent_sales_segment,
    COALESCE(sfdc.parent_crm_account_region, sfdc.crm_person_region)                                                                 AS sfdc_parent_crm_account_region,
    marketo_lead.marketo_lead_id                                                                                                     AS marketo_lead_id,
    IFF(marketo_lead.email_address IS NOT NULL, TRUE, FALSE)                                                                         AS is_marketo_lead,
    COALESCE(marketo_lead.is_marketo_email_bounced, FALSE)                                                                           AS is_marketo_email_hard_bounced,
    marketo_lead.marketo_email_bounced_date                                                                                          AS marketo_email_hard_bounced_date,
    marketo_lead.is_marketo_unsubscribed,
    marketo_lead.is_marketo_opted_in,
    marketo_lead.has_marketo_unsubscribed,
    marketo_lead.marketo_compliance_segment_value                                                                                    AS marketo_compliance_segment_value,
    marketo_lead.sales_segmentation                                                                                                  AS marketo_sales_segmentation,
    COALESCE(marketo_lead.is_pql_marketo, FALSE)                                                                                     AS is_pql_marketo,
    COALESCE(marketo_lead.is_paid_tier_marketo, FALSE)                                                                               AS is_paid_tier_marketo,
    COALESCE(marketo_lead.is_ptpt_contact_marketo, FALSE)                                                                            AS is_ptpt_contact_marketo,
    COALESCE(marketo_lead.is_ptp_contact_marketo, FALSE)                                                                             AS is_ptp_contact_marketo,
    COALESCE(marketo_lead.is_impacted_by_user_limit_marketo, FALSE)                                                                  AS is_impacted_by_user_limit_marketo,
    COALESCE(marketo_lead.is_currently_in_trial_marketo, FALSE)                                                                      AS is_currently_in_trial_marketo,
    marketo_lead.trial_start_date_marketo                                                                                            AS trial_start_date_marketo,
    marketo_lead.trial_end_date_marketo                                                                                              AS trial_end_date_marketo,
    COALESCE (sfdc.email_address IS NOT NULL, FALSE)                                                                                 AS is_sfdc_lead_contact,
    sfdc.sfdc_record_id,
    sfdc.dim_crm_account_id,
    sfdc.sfdc_lead_contact,
    sfdc.contact_role,
    COALESCE(marketo_lead.mobile_phone, sfdc.mobile_phone)                                                                           AS mobile_phone,
    sfdc.sfdc_created_date                                                                                                           AS sfdc_created_date,
    sfdc.opted_out_salesforce                                                                                                        AS is_sfdc_opted_out,
    COALESCE (gitlab_dotcom.email_address IS NOT NULL, FALSE)                                                                        AS is_gitlab_dotcom_user,
    gitlab_dotcom.user_id                                                                                                            AS gitlab_dotcom_user_id,
    gitlab_dotcom.created_date                                                                                                       AS gitlab_dotcom_created_date,
    gitlab_dotcom.confirmed_date                                                                                                     AS gitlab_dotcom_confirmed_date,
    gitlab_dotcom.active_state                                                                                                       AS gitlab_dotcom_active_state,
    gitlab_dotcom.last_login_date                                                                                                    AS gitlab_dotcom_last_login_date,
    gitlab_dotcom.email_opted_in                                                                                                     AS gitlab_dotcom_email_opted_in,
    DATEDIFF(DAY, gitlab_dotcom.confirmed_date, GETDATE())                                                                           AS days_since_saas_signup,
    {{ days_buckets('days_since_saas_signup') }}                                                                                                                                  AS days_since_saas_signup_bucket,
    COALESCE (customer_db.email_address IS NOT NULL, FALSE)                                                                          AS is_customer_db_user,
    customer_db.customer_id                                                                                                          AS customer_db_customer_id,
    customer_db.created_date                                                                                                         AS customer_db_created_date,
    customer_db.confirmed_date                                                                                                       AS customer_db_confirmed_date,
    DATEDIFF(DAY, customer_db.confirmed_date, GETDATE())                                                                             AS days_since_self_managed_owner_signup,
    {{ days_buckets('days_since_self_managed_owner_signup') }}                                                                                                                                  AS days_since_self_managed_owner_signup_bucket,
    COALESCE (zuora.email_address IS NOT NULL, FALSE)                                                                                AS is_zuora_billing_contact,
    zuora.contact_id                                                                                                                 AS zuora_contact_id,
    zuora.created_date                                                                                                               AS zuora_created_date,
    zuora.active_state                                                                                                               AS zuora_active_state,
    dnc_list.result                                                                                                                  AS dnc_list_result,
    NOT COALESCE (dnc_list.result IN ('undeliverable', 'do_not_send'), FALSE)                                                        AS wip_is_valid_email_address,
    CASE
      WHEN NOT wip_is_valid_email_address
        THEN dnc_list.result
    END                                                                                                                              AS wip_invalid_email_address_reason,
    CASE
      WHEN RLIKE(emails.email_address, '[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.ru$')
        THEN TRUE
      WHEN COALESCE(zuora.country, marketo_lead.country, sfdc.country, customer_db.country) IN ('Iran, Islamic Republic of','Cuba','North Korea','Syrian Arab Republic','Russian Federation','Russia','Belarus','Republic of Belarus','Belarus, Republic of')
        THEN TRUE
      ELSE FALSE
    END AS is_in_embargoed_country

  FROM emails
  LEFT JOIN sfdc
    ON emails.email_address = sfdc.email_address
  LEFT JOIN gitlab_dotcom
    ON emails.email_address = gitlab_dotcom.email_address
  LEFT JOIN customer_db
    ON emails.email_address = customer_db.email_address
  LEFT JOIN zuora
    ON emails.email_address = zuora.email_address
  LEFT JOIN marketo_lead
    ON emails.email_address = marketo_lead.email_address
  LEFT JOIN dnc_list
    ON emails.email_address = dnc_list.email_address

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@rmistry",
    updated_by="@rkohnke",
    created_date="2021-01-19",
    updated_date="2024-10-08"
) }}

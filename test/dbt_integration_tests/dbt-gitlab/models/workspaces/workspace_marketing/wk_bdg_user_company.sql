{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('users','gitlab_dotcom_users_source'),
    ('users_enhance','gitlab_contact_enhance_source')
]) }},

sf_leads AS (

  SELECT
    zoominfo_company_id,
    lead_email
  FROM {{ ref('sfdc_lead_source') }}
  WHERE zoominfo_company_id IS NOT NULL
  -- email is not unique, use the record created most recently
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lead_email ORDER BY created_date DESC ) = 1
),

sf_contacts AS (

  SELECT
    zoominfo_company_id,
    contact_email
  FROM {{ ref('sfdc_contact_source') }}
  WHERE zoominfo_company_id IS NOT NULL
  -- email is not unique, use the record created most recently
  QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_email ORDER BY created_date DESC ) = 1
),

rpt AS (

  SELECT DISTINCT
    users.user_id AS gitlab_dotcom_user_id,
    COALESCE(
      sf_leads.zoominfo_company_id,
      sf_contacts.zoominfo_company_id,
      users_enhance.zoominfo_company_id
    ) AS company_id,
    sf_leads.zoominfo_company_id AS sf_lead_company_id,
    sf_contacts.zoominfo_company_id AS sf_contact_company_id,
    users_enhance.zoominfo_company_id AS gitlab_user_enhance_company_id,
    {{ dbt_utils.generate_surrogate_key(['users.user_id']) }} AS dim_user_id,
    {{ dbt_utils.generate_surrogate_key(['company_id']) }} AS dim_company_id
  FROM users
  LEFT JOIN sf_leads
    ON users.email = sf_leads.lead_email
  LEFT JOIN sf_contacts
    ON users.email = sf_contacts.contact_email
  LEFT JOIN users_enhance
    ON users.user_id = users_enhance.user_id
  WHERE company_id IS NOT NULL

)

SELECT * FROM rpt
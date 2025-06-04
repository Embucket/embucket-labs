{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('accounts','sfdc_account_source'),
    ('contacts','sfdc_contact_source'),
    ('leads','sfdc_lead_source'),
    ('zoom_info','zi_comp_with_linkages_global_source')
]) }},

salesforce_accounts AS (
  SELECT
    zoom_info_dozisf_zi_id AS company_id,
    zoom_info_company_name AS company_name,
    zoom_info_company_revenue AS company_revenue,
    zoom_info_company_employee_count AS company_employee_count,
    zoom_info_company_industry AS company_industry,
    zoom_info_company_state_province AS company_state_province,
    zoom_info_company_country AS company_country,
    IFF(company_industry IS NOT NULL, 1, 0)
    + IFF(company_state_province IS NOT NULL, 1, 0 )
    + IFF(company_country IS NOT NULL, 1, 0) AS completeness_score
  FROM accounts
  WHERE company_id IS NOT NULL
    AND is_excluded_from_zoom_info_enrich = FALSE
  QUALIFY MAX(company_revenue) OVER (PARTITION BY company_id) = company_revenue
    AND ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY completeness_score DESC) = 1
),

salesforce_leads AS (
  SELECT
    zoominfo_company_id AS company_id,
    company AS company_name,
    zoominfo_company_revenue AS company_revenue,
    zoominfo_company_employee_count AS company_employee_count,
    zoominfo_company_industry AS company_industry,
    zoominfo_company_state AS company_state_province,
    zoominfo_company_country AS company_country,
    IFF(company_industry IS NOT NULL, 1, 0)
    + IFF(company_state_province IS NOT NULL, 1, 0 )
    + IFF(company_country IS NOT NULL, 1, 0) AS completeness_score
  FROM leads
  WHERE company_id IS NOT NULL
  QUALIFY MAX(company_revenue) OVER (PARTITION BY company_id) = company_revenue
    AND ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY completeness_score DESC) = 1
),

salesforce_contacts AS (
  SELECT
    zoominfo_company_id AS company_id,
    zoominfo_company_revenue AS company_revenue,
    zoominfo_company_employee_count AS company_employee_count,
    zoominfo_company_industry AS company_industry,
    zoominfo_company_state AS company_state_province,
    zoominfo_company_country AS company_country,
    IFF(company_industry IS NOT NULL, 1, 0)
    + IFF(company_state_province IS NOT NULL, 1, 0 )
    + IFF(company_country IS NOT NULL, 1, 0) AS completeness_score
  FROM contacts
  WHERE company_id IS NOT NULL
  QUALIFY MAX(company_revenue) OVER (PARTITION BY company_id) = company_revenue
    AND ROW_NUMBER() OVER (PARTITION BY company_id ORDER BY completeness_score DESC) = 1
),

zoom_info_base AS (
  SELECT
    company_id AS company_id,
    headquarters_company_name AS company_name,
    headquarters_employees AS company_employee_count,
    industry_primary AS company_industry,
    headquarters_company_state AS company_state_province,
    headquarters_company_country AS company_country,
    merged_previous_company_ids,
    headquarters_revenue AS company_revenue
  FROM zoom_info
  WHERE is_headquarters = TRUE
),

zoom_info_merged AS (
  SELECT DISTINCT
    merged_company_ids.value::VARCHAR AS company_id,
    zoom_info_base.company_name,
    zoom_info_base.company_revenue,
    zoom_info_base.company_employee_count,
    zoom_info_base.company_industry,
    zoom_info_base.company_state_province,
    zoom_info_base.company_country,
    zoom_info_base.company_id AS source_company_id
  FROM zoom_info_base
  INNER JOIN LATERAL FLATTEN(INPUT =>SPLIT(merged_previous_company_ids, '|')) AS merged_company_ids
),

company_id_spine AS (

  SELECT company_id
  FROM salesforce_accounts

  UNION

  SELECT company_id
  FROM salesforce_leads

  UNION

  SELECT company_id
  FROM salesforce_contacts

  UNION

  SELECT company_id
  FROM zoom_info_base

  UNION

  SELECT company_id
  FROM zoom_info_merged

),

report AS (
  SELECT DISTINCT
    {{ dbt_utils.generate_surrogate_key(['company_id_spine.company_id::INT']) }} AS dim_company_id,
    company_id_spine.company_id::INT AS company_id,
    zoom_info_merged.source_company_id,
    COALESCE(zoom_info_base.company_name,
      zoom_info_merged.company_name,
      salesforce_accounts.company_name,
      salesforce_leads.company_name,
      'Unknown Company Name') AS company_name,
    COALESCE(zoom_info_base.company_revenue,
      zoom_info_merged.company_revenue,
      salesforce_accounts.company_revenue,
      salesforce_contacts.company_revenue,
      salesforce_leads.company_revenue) AS company_revenue,
    COALESCE(zoom_info_base.company_employee_count,
      zoom_info_merged.company_employee_count,
      salesforce_accounts.company_employee_count,
      salesforce_contacts.company_employee_count,
      salesforce_leads.company_employee_count) AS company_employee_count,
    COALESCE(zoom_info_base.company_industry,
      zoom_info_merged.company_industry,
      salesforce_accounts.company_industry,
      salesforce_contacts.company_industry,
      salesforce_leads.company_industry) AS company_industry,
    COALESCE(zoom_info_base.company_country,
      zoom_info_merged.company_country,
      salesforce_accounts.company_country,
      salesforce_contacts.company_country,
      salesforce_leads.company_country) AS company_country,
    COALESCE(zoom_info_base.company_state_province,
      zoom_info_merged.company_state_province,
      salesforce_accounts.company_state_province,
      salesforce_contacts.company_state_province,
      salesforce_leads.company_state_province) AS company_state_province,
    IFF(
      salesforce_accounts.company_id IS NOT NULL, TRUE, FALSE
    ) AS has_crm_account,
    IFF(
      salesforce_leads.company_id IS NOT NULL, TRUE, FALSE
    ) AS has_crm_lead,
    IFF(
      salesforce_contacts.company_id IS NOT NULL, TRUE, FALSE
    ) AS has_crm_contact,
    IFF(
      zoom_info_base.company_id IS NOT NULL, TRUE, FALSE
    ) AS is_company_hq,
    IFF(zoom_info_merged.company_id IS NOT NULL, TRUE, FALSE) AS is_merged_company_id
  FROM company_id_spine
  LEFT JOIN zoom_info_base
    ON company_id_spine.company_id = zoom_info_base.company_id
  LEFT JOIN salesforce_accounts
    ON company_id_spine.company_id = salesforce_accounts.company_id
  LEFT JOIN zoom_info_merged
    ON company_id_spine.company_id = zoom_info_merged.company_id
  LEFT JOIN salesforce_leads
    ON company_id_spine.company_id = salesforce_leads.company_id
  LEFT JOIN salesforce_contacts
    ON company_id_spine.company_id = salesforce_contacts.company_id
  WHERE company_id_spine.company_id IS NOT NULL
)

SELECT *
FROM report

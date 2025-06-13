
{{ config({
    "tags": ["mnpi_exception"]
    })
}}

WITH map_free_instances_to_crm_account_prep AS (
  
    SELECT *
    FROM {{ ref('map_free_instances_to_crm_account_prep') }}

), sheetload_hostnames_to_domains AS (

    SELECT DISTINCT 
      host_name,
      cleaned_company_domain,
      dim_crm_account_id
    FROM {{ ref('sheetload_hostnames_to_domains') }}

), dim_crm_account AS (

    SELECT dim_crm_account_id, crm_account_name
    FROM {{ ref('dim_crm_account') }}

)

SELECT
    sheetload_hostnames_to_domains.host_name,
    NULL AS host_name_chopped,
    NULL AS extracted_name,
    NULL AS extracted_name_cleaned,
    sheetload_hostnames_to_domains.dim_crm_account_id AS matched_dim_crm_account_id,
    dim_crm_account.crm_account_name AS matched_crm_account_name,
    NULL AS match_count,
    NULL AS match_quality,
    NULL AS match,
    NULL AS is_educational,
    NULL AS is_ip,
    NULL AS last_date_host_name_seen,
    NULL AS processed_date,
    'Claude' AS source

FROM sheetload_hostnames_to_domains
LEFT JOIN dim_crm_account
    ON sheetload_hostnames_to_domains.dim_crm_account_id = dim_crm_account.dim_crm_account_id

UNION

SELECT 
    map_free_instances_to_crm_account_prep.host_name,
    map_free_instances_to_crm_account_prep.host_name_chopped,
    map_free_instances_to_crm_account_prep.extracted_name,
    map_free_instances_to_crm_account_prep.extracted_name_cleaned,
    map_free_instances_to_crm_account_prep.matched_dim_crm_account_id,
    map_free_instances_to_crm_account_prep.matched_crm_account_name,
    map_free_instances_to_crm_account_prep.match_count,
    map_free_instances_to_crm_account_prep.match_quality,
    map_free_instances_to_crm_account_prep.match,
    map_free_instances_to_crm_account_prep.is_educational,
    map_free_instances_to_crm_account_prep.is_ip,
    map_free_instances_to_crm_account_prep.last_date_host_name_seen,
    map_free_instances_to_crm_account_prep.processed_date,
    'Algorithm Reconciliation' AS source
FROM map_free_instances_to_crm_account_prep
LEFT JOIN sheetload_hostnames_to_domains
    ON map_free_instances_to_crm_account_prep.host_name = sheetload_hostnames_to_domains.host_name
WHERE sheetload_hostnames_to_domains.host_name IS NULL

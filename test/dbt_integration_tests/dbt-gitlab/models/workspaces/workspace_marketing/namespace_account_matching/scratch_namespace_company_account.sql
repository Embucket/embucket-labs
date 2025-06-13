{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

with 

namespaces AS (
  SELECT *
  FROM {{ ref('dim_namespace') }}
),

customer_account_company AS (
  SELECT *
  FROM {{ ref('customer_account_company') }}
),

top_scoreing_account AS (
  SELECT *
  FROM {{ ref('account_score') }}
),

top_scoreing_company AS (
  SELECT *
  FROM {{ ref('company_score') }}
),

combined AS (
  SELECT
    namespaces.dim_namespace_id AS namespace_id,
    namespaces.namespace_type,
    namespaces.gitlab_plan_title,
    COALESCE(customer_account_company.sfdc_account_id, top_scoreing_account.crm_account_id) AS account_id,
    COALESCE(customer_account_company.company_id, top_scoreing_company.company_id) AS company_id,
    customer_account_company.sfdc_account_id AS customer_account_id,
    customer_account_company.company_id AS customer_company_id,
    top_scoreing_account.crm_account_id AS user_account_id,
    top_scoreing_company.company_id AS user_company_id,
    top_scoreing_company.combined_score AS company_combined_score,
    top_scoreing_account.combined_score AS account_combined_score
  FROM prod.common.dim_namespace namespaces
  LEFT JOIN customer_account_company
    ON namespaces.dim_namespace_id = customer_account_company.ultimate_parent_id
  LEFT JOIN top_scoreing_account
    ON namespaces.dim_namespace_id = top_scoreing_account.namespace_id
  LEFT JOIN top_scoreing_company
    ON namespaces.dim_namespace_id = top_scoreing_company.namespace_id
  WHERE namespace_is_ultimate_parent
)


select * from combined
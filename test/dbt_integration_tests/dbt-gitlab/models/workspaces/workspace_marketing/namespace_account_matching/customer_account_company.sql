{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

WITH 
customer_orders AS (
  SELECT *
  FROM {{ ref('customers_db_orders_source') }}
),
namespace_lineage AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_namespace_lineage_scd') }}
),

customers AS (
  SELECT *
  FROM {{ ref('customers_db_customers_source') }}
),

crm_account AS (
  SELECT *
  FROM {{ ref('dim_crm_account') }}
),

customer_account_company AS (
  SELECT DISTINCT
    --customer_orders.customer_id,
    --customer_orders.gitlab_namespace_id,
    namespace_lineage.ultimate_parent_id,
    customers.sfdc_account_id,
    crm_account.crm_account_zoom_info_dozisf_zi_id AS company_id,
    IFF(customers.sfdc_account_id IS NOT NULL,1,0) +
      IFF(crm_account.crm_account_zoom_info_dozisf_zi_id IS NOT NULL,1,0) AS completeness_score
  FROM customer_orders
  LEFT JOIN namespace_lineage
    ON customer_orders.gitlab_namespace_id = namespace_lineage.namespace_id
    AND customer_orders.order_updated_at::DATE
      BETWEEN namespace_lineage.lineage_valid_from::DATE AND namespace_lineage.lineage_valid_to::DATE
  LEFT JOIN customers
    ON customer_orders.customer_id = customers.customer_id
  LEFT JOIN crm_account
    ON customers.sfdc_account_id = crm_account.dim_crm_account_id
  WHERE customer_orders.gitlab_namespace_id IS NOT NULL
  AND completeness_score !=0
  QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace_lineage.ultimate_parent_id ORDER BY completeness_score DESC) = 1
)

SELECT *
FROM customer_account_company
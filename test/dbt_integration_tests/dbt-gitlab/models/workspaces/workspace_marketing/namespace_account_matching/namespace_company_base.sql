{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

with 
memberships AS (
  SELECT *
  FROM {{ ref('gitlab_dotcom_memberships') }}
),

namespace AS (
  SELECT *
  FROM {{ ref('dim_namespace') }}
),

company_bridge AS (
  SELECT *
  FROM {{ ref('wk_bdg_user_company') }}
),

marketing_contact AS (
  SELECT *
  FROM {{ ref('dim_marketing_contact') }}
),

crm_company AS (
  SELECT *
  FROM {{ ref('dim_crm_account') }}
),

namespace_companies AS (
  SELECT
    memberships.user_id,
    memberships.user_type,
    memberships.membership_source_type,
    memberships.access_level,
    memberships.ultimate_parent_id,
    namespace.namespace_type,
    namespace.creator_id,
    CASE
      WHEN memberships.user_id = namespace.creator_id THEN 'creator'
      WHEN memberships.access_level = 50 AND memberships.membership_source_type = 'group_membership' THEN 'owner'
      WHEN memberships.access_level = 50 THEN 'owner'
      ELSE 'user'
    END AS relationship_type,
    IFF(namespace.creator_id = memberships.user_id, TRUE, FALSE) AS is_creator,
    IFF(access_level = 50, TRUE, FALSE) AS is_owner,
    company_bridge.company_id,
    COALESCE(marketing_contact.dim_crm_account_id, crm_company.dim_crm_account_id) AS crm_account_id
  FROM memberships
  LEFT JOIN namespace
    ON memberships.ultimate_parent_id = namespace.dim_namespace_id
  LEFT JOIN company_bridge
    ON memberships.user_id = company_bridge.gitlab_dotcom_user_id
  LEFT JOIN marketing_contact
    ON memberships.user_id = marketing_contact.gitlab_dotcom_user_id
  LEFT JOIN crm_company
    ON company_bridge.company_id = crm_company.crm_account_zoom_info_dozisf_zi_id
  WHERE true
    and memberships.is_billable
  QUALIFY ROW_NUMBER() OVER (PARTITION BY ultimate_parent_id,user_id ORDER BY access_level DESC) = 1
)

SELECT *
FROM namespace_companies

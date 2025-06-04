{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

/*
This table is a derived fct table from a future fct_membership.
This table has a grain of user_id->namespace_id->crm_account_id->company_id
This table assumes the highest access level for the user on the namespace
*/
{{ simple_cte([
    ('memberships','gitlab_dotcom_memberships'),
    ('namespace','dim_namespace'),
    ('company_bridge','wk_bdg_user_company'),
    ('marketing_contact','dim_marketing_contact'),
    ('access_levels','gitlab_dotcom_access_levels_source')
]) }},

namespace_companies_accounts AS (

  SELECT
    -- Primary Key
    {{ dbt_utils.generate_surrogate_key(['memberships.user_id',
      'namespace.dim_namespace_id',
      'company_bridge.company_id',
      'marketing_contact.dim_crm_account_id']) }} AS user_namespace_account_company_pk,

    -- Foreign Keys
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['memberships.user_id'])) }} AS dim_user_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['namespace.dim_namespace_id'])) }} AS dim_namespace_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['company_bridge.company_id'])) }} AS dim_company_sk,
    {{ get_keyed_nulls(dbt_utils.generate_surrogate_key(['marketing_contact.dim_crm_account_id'])) }} AS dim_crm_account_sk,

    -- Legacy Keys
    memberships.user_id,
    namespace.dim_namespace_id AS namespace_id,
    company_bridge.company_id,
    marketing_contact.dim_crm_account_id AS crm_account_id,

    -- Degenerate Dimensions
    memberships.is_billable,
    memberships.access_level,
    access_levels.access_level_name,
    IFF(namespace.creator_id = memberships.user_id, TRUE, FALSE) AS is_creator,
    IFF(memberships.access_level = 50, TRUE, FALSE) AS is_owner

    -- Facts   

  FROM memberships
  LEFT JOIN namespace
    ON memberships.namespace_id = namespace.dim_namespace_id
  LEFT JOIN company_bridge
    ON memberships.user_id = company_bridge.gitlab_dotcom_user_id
  LEFT JOIN marketing_contact
    ON memberships.user_id = marketing_contact.gitlab_dotcom_user_id
  LEFT JOIN access_levels
    ON memberships.access_level = access_levels.access_level_id
  QUALIFY ROW_NUMBER() OVER (PARTITION BY namespace.dim_namespace_id,
    memberships.user_id ORDER BY memberships.access_level DESC) = 1
)

SELECT *
FROM namespace_companies_accounts

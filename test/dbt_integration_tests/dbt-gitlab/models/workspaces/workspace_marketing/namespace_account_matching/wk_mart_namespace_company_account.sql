{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('company_domain_account','bdg_company_domain_account'),
    ('crm_account','dim_crm_account'),
    ('company','wk_dim_company'),
    ('namespace_order_subscription_monthly','bdg_namespace_order_subscription_monthly'),
    ('namespaces','dim_namespace'),
    ('users','dim_user')
]) }},

user_namespace_account_company AS (
  SELECT *
  FROM {{ ref('wk_fct_user_namespace_account_company') }}
  WHERE is_billable = TRUE
),

domain_matching AS (
  -- Joins dimension tables and changes the grain to the ultimate parent namespace
  SELECT
    user_namespace_account_company.user_id,
    user_namespace_account_company.is_creator,
    user_namespace_account_company.is_owner,
    namespaces.ultimate_parent_namespace_id,
    company.source_company_id,
    users.email_domain,
    users.email_domain_classification,
    COALESCE(
      user_namespace_account_company.crm_account_id,
      company_domain_account.crm_account_id
    ) AS crm_account_id
  FROM user_namespace_account_company
  LEFT JOIN namespaces
    ON user_namespace_account_company.namespace_id = namespaces.dim_namespace_id
  LEFT JOIN company
    ON user_namespace_account_company.company_id = company.company_id
  LEFT JOIN users
    ON user_namespace_account_company.user_id = users.dim_user_id
  LEFT JOIN company_domain_account
    ON company.source_company_id = company_domain_account.company_id
      AND users.email_domain = company_domain_account.email_domain
  QUALIFY ROW_NUMBER() OVER (PARTITION BY
    namespaces.ultimate_parent_namespace_id,
    user_namespace_account_company.user_id
    ORDER BY user_namespace_account_company.access_level ) = 1
),

top_namespace_domain AS (
  -- Find the top used user email domains for the ultimate parent namespace for matching
  SELECT
    domain_matching.ultimate_parent_namespace_id,
    domain_matching.email_domain,
    IFF(domain_matching.email_domain_classification IS NULL, TRUE, FALSE) AS is_business_email,
    COUNT(DISTINCT domain_matching.user_id) AS number_of_users,
    ROW_NUMBER() OVER (
      PARTITION BY domain_matching.ultimate_parent_namespace_id
      ORDER BY number_of_users DESC) AS domain_rank
  FROM domain_matching
  WHERE is_business_email = TRUE
  GROUP BY 1, 2, 3
  QUALIFY domain_rank = 1

),

namespace_domain_account AS (
  -- Finds the crm account related to the top namespace email domain.
  SELECT DISTINCT
    top_namespace_domain.ultimate_parent_namespace_id,
    top_namespace_domain.email_domain,
    company_domain_account.crm_account_id,
    company_domain_account.account_domain_rank,
    ROW_NUMBER() OVER (PARTITION BY
      top_namespace_domain.ultimate_parent_namespace_id,
      top_namespace_domain.email_domain
      ORDER BY company_domain_account.account_domain_rank DESC,
        company_domain_account.crm_account_id ASC) AS namespace_account_rank
  FROM top_namespace_domain
  INNER JOIN company_domain_account
    ON top_namespace_domain.email_domain = company_domain_account.email_domain
  QUALIFY namespace_account_rank = 1


),

namespace_company AS (
  -- Measures and applies logic for determining the best match for a namespace and company
  SELECT
    ultimate_parent_namespace_id,
    source_company_id,
    CASE
      WHEN source_company_id IS NOT NULL THEN 1
      ELSE 0
    END AS has_company,
    MAX(IFF(is_creator, 1, 0)) AS has_creator,
    COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
    COUNT(DISTINCT user_id) AS total_users,
    SUM(total_owners) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners,
    SUM(total_users) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users,
    SUM(IFF(source_company_id IS NOT NULL, total_owners, 0)) OVER (
      PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners_qualified,
    SUM(IFF(source_company_id IS NOT NULL, total_users, 0)) OVER (
      PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users_qualified,
    SUM(IFF(source_company_id IS NOT NULL, has_creator, 0)) OVER (
      PARTITION BY ultimate_parent_namespace_id) AS total_namespace_creator_qualified,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_company DESC, has_creator DESC, total_owners DESC,
        total_users DESC) AS company_rank,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_company DESC, total_owners DESC, total_users DESC) AS company_owner_rank,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_company DESC, total_users DESC) AS company_user_rank,

    IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
        0) AS owner_percent_qualified,
    IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
        0) AS users_percent_qualified,
    IFF(total_namespace_owners > 0, total_owners / total_namespace_owners, 0) AS owner_percent,
    IFF(total_namespace_users > 0, total_users / total_namespace_users, 0) AS users_percent,

    -- Matching logic ratios derived from an evaluation of the results of the query
    IFF(company_rank = 1
      AND has_company = 1
      AND (
        (
          has_creator = 1
          AND company_owner_rank = 1
          AND company_user_rank = 1
          AND owner_percent_qualified > 0.5
          AND users_percent_qualified > 0.5
          AND owner_percent > 0.2
          AND users_percent > 0.2
        )
        OR (
          has_creator = 0
          AND company_owner_rank = 1
          AND company_user_rank = 1
          AND total_namespace_creator_qualified = 0
          AND owner_percent_qualified > 0.6
          AND users_percent_qualified > 0.6
          AND owner_percent > 0.33
          AND users_percent > 0.33
        )
      ), TRUE, FALSE) AS is_best_match
  FROM domain_matching
  GROUP BY 1, 2, 3

),


namespace_account AS (
  -- Measures and applies logic for determining the best match for a namespace and crm account
  SELECT
    ultimate_parent_namespace_id,
    crm_account_id,
    CASE
      WHEN crm_account_id IS NOT NULL THEN 1
      ELSE 0
    END AS has_account,
    MAX(IFF(is_creator, 1, 0)) AS has_creator,
    COUNT(DISTINCT IFF(is_owner, user_id, NULL)) AS total_owners,
    COUNT(DISTINCT user_id) AS total_users,
    SUM(total_owners) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners,
    SUM(total_users) OVER (PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users,
    SUM(IFF(crm_account_id IS NOT NULL, total_owners, 0)) OVER (
      PARTITION BY ultimate_parent_namespace_id) AS total_namespace_owners_qualified,
    SUM(IFF(crm_account_id IS NOT NULL, total_users, 0)) OVER (
      PARTITION BY ultimate_parent_namespace_id) AS total_namespace_users_qualified,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, has_creator DESC, total_owners DESC,
        total_users DESC) AS account_rank,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, total_owners DESC, total_users DESC) AS account_owner_rank,
    ROW_NUMBER() OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, total_users DESC) AS account_user_rank,
    NTH_VALUE(total_users, 1) OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, total_users DESC) AS users_count_1st,
    NTH_VALUE(total_users, 2) OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, total_users DESC) AS users_count_2nd,
    NTH_VALUE(has_account, 2) OVER (
      PARTITION BY ultimate_parent_namespace_id
      ORDER BY has_account DESC, total_users DESC) AS users_count_2nd_has_account,
    IFF(total_namespace_owners_qualified > 0, total_owners / total_namespace_owners_qualified,
        0) AS owner_percent_qualified,
    --IFF(total_namespace_users_qualified > 0, total_users / total_namespace_users_qualified,
        --0) AS users_percent_qualified, -- ?

    IFF(
      account_user_rank = 1
      AND total_users >= 1
      AND has_account = 1
      AND (
        COALESCE(users_count_2nd_has_account, 0) = 0
        OR (
          users_count_1st > COALESCE(users_count_2nd, 0)
          AND COALESCE(users_count_2nd_has_account, 1) = 1
        )
      ), TRUE, FALSE) AS is_most_matched_account,
    IFF(
      account_rank = 1
      AND has_account = 1
      AND (
        (
          has_creator = 1
          AND account_owner_rank = 1
          AND owner_percent_qualified > 0.5
        )
        OR (
          has_creator = 0
          AND account_owner_rank = 1
          AND owner_percent_qualified > 0.66
        )
      ), TRUE, FALSE) AS is_best_match
  FROM domain_matching
  GROUP BY 1, 2, 3
),

subscription_accounts AS (
  -- Finds the direct link between namespace and crm account.
  SELECT DISTINCT
    dim_namespace_id AS namespace_id,
    dim_crm_account_id AS crm_account_id
  FROM namespace_order_subscription_monthly
  WHERE dim_crm_account_id IS NOT NULL
    AND dim_namespace_id IS NOT NULL
    AND snapshot_month <= DATE_TRUNC('month', CURRENT_DATE())
  QUALIFY MAX(snapshot_month) OVER (PARTITION BY dim_namespace_id) = snapshot_month
    AND ROW_NUMBER() OVER (PARTITION BY dim_namespace_id, snapshot_month ORDER BY order_start_date DESC) = 1
),

direct_company_account AS (
  -- Finds the source company that is associated with the direct link crm account
  SELECT
    subscription_accounts.namespace_id,
    crm_account.dim_crm_account_id AS crm_account_id,
    company.source_company_id
  FROM subscription_accounts
  LEFT JOIN crm_account
    ON subscription_accounts.crm_account_id = crm_account.dim_crm_account_id
  LEFT JOIN company
    ON crm_account.crm_account_zoom_info_dozisf_zi_id = company.company_id
),

source_company_accounts AS (
  -- Finds all of the crm account related to a source company id
  SELECT
    company.source_company_id,
    COUNT(DISTINCT crm_account.dim_crm_account_id) AS number_of_accounts,
    COUNT(DISTINCT crm_account.dim_parent_crm_account_id) AS number_of_parent_accounts,
    ARRAY_AGG(DISTINCT CONCAT(crm_account.dim_crm_account_id, ':',
      crm_account.crm_account_name)) AS list_of_accounts,
    ARRAY_AGG(DISTINCT CONCAT(crm_account.dim_parent_crm_account_id, ':',
      crm_account.parent_crm_account_name)) AS list_of_parent_accounts
  FROM crm_account
  LEFT JOIN company
    ON crm_account.crm_account_zoom_info_dozisf_zi_id::VARCHAR = company.company_id::VARCHAR
  GROUP BY 1

),

mart AS (

  SELECT
    namespaces.namespace_type,
    namespaces.creator_id,
    namespaces.visibility_level,
    namespaces.dim_namespace_id AS namespace_id,
    namespaces.gitlab_plan_title,
    namespaces.gitlab_plan_is_paid,
    namespaces.is_setup_for_company,
    namespaces.created_at,
    namespaces.namespace_is_ultimate_parent,
    namespaces.namespace_is_internal,
    namespaces.namespace_creator_is_blocked,
    source_company_accounts.number_of_accounts,
    source_company_accounts.number_of_parent_accounts,
    source_company_accounts.list_of_accounts,
    source_company_accounts.list_of_parent_accounts,
    direct_company_account.crm_account_id AS actual_crm_account_id,
    direct_company_account.source_company_id AS actual_company_id,
    CASE
      WHEN users.dim_user_id IS NULL THEN 'Missing'
      ELSE COALESCE(users.email_domain_classification, 'Business')
    END AS email_domain_type,
    IFF(namespace_company.ultimate_parent_namespace_id IS NOT NULL,
      namespace_company.source_company_id, NULL) AS predicted_company_id,
    IFF(namespace_account.ultimate_parent_namespace_id IS NOT NULL,
      namespace_account.crm_account_id, NULL) AS predicted_crm_account_id,
    COALESCE(actual_crm_account_id, predicted_crm_account_id) AS namespace_crm_account_id,
    COALESCE(actual_company_id, predicted_company_id) AS namespace_company_id,
    COALESCE(
      IFF(source_company_accounts.number_of_accounts = 1,
        source_company_accounts.list_of_accounts[0], NULL),
      namespace_domain_account.crm_account_id) AS company_linked_account,
    COALESCE(namespace_crm_account_id, company_linked_account) AS combined_crm_account_id,
    CASE
      WHEN actual_crm_account_id IS NOT NULL THEN 'actual_account'
      WHEN predicted_crm_account_id IS NOT NULL THEN 'predicted_account'
      WHEN company_linked_account IS NOT NULL THEN 'company_linked_account'
      WHEN source_company_accounts.number_of_accounts > 1 THEN 'company_linked_multi_account'
      WHEN predicted_company_id IS NOT NULL THEN 'company_linked_but_no_account_match'
      ELSE 'None'
    END AS match_account_type
  FROM namespaces
  LEFT JOIN users
    ON users.dim_user_id = namespaces.creator_id
  LEFT JOIN direct_company_account
    ON direct_company_account.namespace_id = namespaces.dim_namespace_id
  LEFT JOIN namespace_company
    ON namespace_company.ultimate_parent_namespace_id = namespaces.dim_namespace_id
      AND namespace_company.is_best_match = TRUE
  LEFT JOIN namespace_account
    ON namespace_account.ultimate_parent_namespace_id = namespaces.dim_namespace_id
      AND namespace_account.is_best_match = TRUE
  LEFT JOIN namespace_domain_account
    ON namespaces.dim_namespace_id = namespace_domain_account.ultimate_parent_namespace_id
  LEFT JOIN source_company_accounts
    ON source_company_accounts.source_company_id = COALESCE(
      direct_company_account.source_company_id, namespace_company.source_company_id)

)

SELECT *
FROM mart

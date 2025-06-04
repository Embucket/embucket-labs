{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

WITH namespace_companies AS (
  SELECT *
  FROM {{ ref('namespace_company_base') }}
),

account_score AS (
  SELECT DISTINCT
    ultimate_parent_id AS namespace_id,

    --relationship_type,
    crm_account_id,
    COUNT(DISTINCT user_id) OVER (PARTITION BY ultimate_parent_id) AS total_namespace_of_users,
    COUNT(DISTINCT crm_account_id) OVER (PARTITION BY ultimate_parent_id) AS total_namespace_of_accounts,
    COUNT(DISTINCT IFF(relationship_type = 'creator', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_creators,
    COUNT(DISTINCT IFF(relationship_type = 'owner', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_owners,
    COUNT(DISTINCT IFF(relationship_type = 'user', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_users,
    (1/3) AS creator_weight,
    (1/3) AS owner_weight,
    (1/3) AS user_weight,
    COUNT(DISTINCT IFF(relationship_type = 'owner', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,crm_account_id) AS number_of_owners,
    COUNT(DISTINCT IFF(relationship_type = 'creator', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,crm_account_id) AS number_of_creators,
    COUNT(DISTINCT IFF(relationship_type = 'user', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,crm_account_id) AS number_of_users,
    number_of_owners + number_of_creators + number_of_users AS number_of_account_users,
    number_of_account_users / total_namespace_of_users AS account_score,
    (IFF(crm_account_id IS NULL, 0, 1)) AS consider_no_account_multiplier,
    (
        (ZEROIFNULL(number_of_creators / NULLIFZERO(total_namespace_creators)) * creator_weight) +
        (ZEROIFNULL(number_of_owners / NULLIFZERO(total_namespace_owners)) * owner_weight) +
        (ZEROIFNULL(number_of_users / NULLIFZERO(total_namespace_users)) * user_weight)
      ) AS user_score,
    ((user_score + account_score) / 2) * 100 AS combined_score
  FROM namespace_companies
),

top_scoring_account AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY account_score DESC) AS account_score_rank,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY user_score DESC) AS user_score_rank,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY combined_score DESC) AS combined_score_rank
  FROM account_score
  where consider_no_account_multiplier = 1
    QUALIFY combined_score_rank = 1 --AND user_score_rank !=1
  ORDER BY combined_score_rank
)

SELECT *
FROM top_scoring_account
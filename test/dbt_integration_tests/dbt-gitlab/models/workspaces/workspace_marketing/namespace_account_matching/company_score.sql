{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

WITH namespace_companies AS (
  SELECT *
  FROM {{ ref('namespace_company_base') }}
),

company_score AS (
  SELECT DISTINCT
    ultimate_parent_id AS namespace_id,

    --relationship_type,
    company_id,
    COUNT(DISTINCT user_id) OVER (PARTITION BY ultimate_parent_id) AS total_namespace_of_users,
    COUNT(DISTINCT company_id) OVER (PARTITION BY ultimate_parent_id) AS total_namespace_of_compaines,
    COUNT(DISTINCT IFF(relationship_type = 'creator', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_creators,
    COUNT(DISTINCT IFF(relationship_type = 'owner', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_owners,
    COUNT(DISTINCT IFF(relationship_type = 'user', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id) AS total_namespace_users,
    0.60 AS creator_weight,
    0.30 AS owner_weight,
    0.10 AS user_weight,
    COUNT(DISTINCT IFF(relationship_type = 'owner', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,company_id) AS number_of_owners,
    COUNT(DISTINCT IFF(relationship_type = 'creator', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,company_id) AS number_of_creators,
    COUNT(DISTINCT IFF(relationship_type = 'user', user_id, NULL))
          OVER (PARTITION BY ultimate_parent_id,company_id) AS number_of_users,
    number_of_owners + number_of_creators + number_of_users AS number_of_company_users,
    ZEROIFNULL(number_of_creators / NULLIFZERO(total_namespace_creators)) AS creator_fraction,
    ZEROIFNULL(number_of_owners / NULLIFZERO(total_namespace_owners)) AS owner_fraction,
    ZEROIFNULL(number_of_users / NULLIFZERO(total_namespace_users)) AS user_fraction,
    (IFF(company_id IS NULL, 0, 1)) AS consider_no_company_multiplier,
    IFF(number_of_company_users / total_namespace_of_users > 0.5,1,0) AS company_score,
    (
      IFF(number_of_creators > 0 AND number_of_users > 0, 1, 0) +
      IFF(number_of_creators > 0 AND number_of_owners > 0, 1, 0) +
      IFF(number_of_owners > 0 AND number_of_users > 0, 1, 0)
    ) / 3 AS simolarity_score,
    (
        (creator_fraction * creator_weight) +
        (owner_fraction * owner_weight) +
        (user_fraction * user_weight)
      ) AS user_score,
    ((user_score + company_score + simolarity_score) / 3) * 100 AS combined_score
  FROM namespace_companies
  
),

top_scoring_company AS (
  SELECT
    *,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY company_score DESC) AS company_score_rank,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY user_score DESC) AS user_score_rank,
    ROW_NUMBER() OVER (PARTITION BY namespace_id ORDER BY combined_score DESC) AS combined_score_rank,
    -- Charan compare
    IFF(company_id IS NULL,NULL, number_of_users) AS only_company_users,
    IFF(company_id IS NULL,NULL, number_of_owners) AS only_company_owners,
    NTH_VALUE(only_company_users,1)  OVER (PARTITION BY namespace_id ORDER BY only_company_users DESC) AS higest_company_users,
    NTH_VALUE(only_company_owners,1)  OVER (PARTITION BY namespace_id ORDER BY only_company_owners DESC) AS higest_company_owners,
    COALESCE(NTH_VALUE(only_company_users,2)  OVER (PARTITION BY namespace_id ORDER BY only_company_users DESC),0) as second_higest_company_users,
    COALESCE(NTH_VALUE(only_company_owners,2)  OVER (PARTITION BY namespace_id ORDER BY only_company_owners DESC),0) as second_higest_company_owners,
    number_of_creators > 0 AS is_creator_company,
    number_of_owners > 0 AS is_owner_company,
    number_of_users > 0 AS is_user_company,
    number_of_users = higest_company_users AS is_higest_company_users,
    number_of_owners = higest_company_owners AS is_higest_company_owners,
    total_namespace_of_compaines = 1 AS is_only_company,
    ZEROIFNULL(ROUND(number_of_company_users * 1.0 / NULLIFZERO(total_namespace_of_users), 2)) >= 0.5 AS is_most_users,
    ZEROIFNULL(ROUND(number_of_owners * 1.0 / NULLIFZERO(total_namespace_owners), 2)) >= 0.5 AS is_most_owners,
    number_of_users = second_higest_company_users AS is_second_higest_company_users,
    higest_company_users > second_higest_company_users AS is_first_users_more_than_second,
    higest_company_owners > second_higest_company_owners AS is_first_owners_more_than_second,
    CASE
      -- creator and user company
          WHEN is_creator_company AND is_higest_company_users  AND is_only_company AND is_most_users
            THEN 7 -- Creator and most dominant user company is the same. THere is also only one company match and more than half of users are mapped to this company
          WHEN is_creator_company AND is_higest_company_users AND is_only_company
            THEN 6 -- Creator and most dominant user company is the same. THere is also only one company match
          WHEN is_creator_company AND is_higest_company_users AND
               is_first_users_more_than_second AND is_most_users
            THEN 6 -- Creator and most dominant user company is the same. More than half of users are mapped to this company
      -- owner and user company
          WHEN is_owner_company AND is_higest_company_users AND is_only_company AND is_most_users
            THEN 7 -- Most dominant owner company and most dominant user company is the same. THere is also only one company match and more than half of users are mapped to this company
          WHEN is_owner_company AND is_higest_company_users AND
               is_first_users_more_than_second AND is_most_users
            THEN 6 -- Most dominant owner company and most dominant user company is the same. More than half of users are mapped to this company
      -- owner
          WHEN is_owner_company AND is_higest_company_owners AND is_first_owners_more_than_second AND is_most_owners
            THEN 5 -- most of the owners belong to this company
          WHEN is_owner_company AND is_creator_company AND
               is_first_owners_more_than_second
            THEN 4
      -- user
          WHEN is_creator_company AND is_user_company AND is_first_users_more_than_second
            THEN 4
      -- does it have any match
          WHEN is_creator_company AND
               (is_user_company OR is_owner_company)
            THEN 3 -- atleast one person in the namespace has a company
          WHEN is_owner_company
            THEN 2 -- atleast one person in the namespace has a company
          WHEN is_user_company
            THEN 1 -- atleast one person in the namespace has a company
          ELSE 0
        END AS max_match_type
  FROM company_score
  WHERE true
  and consider_no_company_multiplier = 1
    QUALIFY combined_score_rank = 1 --AND user_score_rank !=1
    --QUALIFY count(*) OVER (PARTITION BY namespace_id) > 1 --AND max_match_type in (7,6,5)
  ORDER BY combined_score_rank
)

SELECT *
FROM top_scoring_company
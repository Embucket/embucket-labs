

WITH namespaces AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespaces_source') }}

), gitlab_subscriptions AS (

    SELECT *
    FROM {{ref('gitlab_dotcom_gitlab_subscriptions')}}
    WHERE is_currently_valid = TRUE

), joined AS (

  SELECT
    namespaces.*,
    COALESCE(gitlab_subscriptions.plan_id, 34) AS plan_id
  FROM namespaces
    LEFT JOIN gitlab_subscriptions
      ON namespaces.namespace_id = gitlab_subscriptions.namespace_id

)

SELECT *
FROM joined

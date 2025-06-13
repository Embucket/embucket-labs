WITH source AS (

    SELECT 
      *,
      IFF(LOWER(last_name) LIKE '%integration%', 1, 0) AS is_integration_user
    FROM {{ref('zuora_query_api_users_source')}}

)

SELECT *
FROM source

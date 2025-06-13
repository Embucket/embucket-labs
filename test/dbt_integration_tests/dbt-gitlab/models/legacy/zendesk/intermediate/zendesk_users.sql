{{config({
    "schema": "legacy"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_users_source') }}
)

SELECT *
FROM source

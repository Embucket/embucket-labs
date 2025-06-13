WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_account_keyword_intent_source') }}

)

SELECT *
FROM source
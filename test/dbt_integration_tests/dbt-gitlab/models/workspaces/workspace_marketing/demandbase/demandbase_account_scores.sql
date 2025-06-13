WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_account_scores_source') }}

)

SELECT *
FROM source
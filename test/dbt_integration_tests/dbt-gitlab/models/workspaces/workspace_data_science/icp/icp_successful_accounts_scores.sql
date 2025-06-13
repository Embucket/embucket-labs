WITH source AS (

    SELECT *
    FROM {{ ref('icp_successful_accounts_scores_source') }}

)

SELECT *
FROM source
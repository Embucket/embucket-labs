WITH source AS (

    SELECT *
    FROM {{ ref('pte_scores_source') }}

)

SELECT *
FROM source
WITH source AS (

    SELECT *
    FROM {{ ref('ptpl_scores_source') }}

)

SELECT *
FROM source
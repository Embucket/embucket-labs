WITH source AS (

    SELECT *
    FROM {{ ref('ptc_scores_source') }}

)

SELECT *
FROM source
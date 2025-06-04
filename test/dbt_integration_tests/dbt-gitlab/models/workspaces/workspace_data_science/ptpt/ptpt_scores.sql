WITH source AS (

    SELECT *
    FROM {{ ref('ptpt_scores_source') }}

)

SELECT *
FROM source
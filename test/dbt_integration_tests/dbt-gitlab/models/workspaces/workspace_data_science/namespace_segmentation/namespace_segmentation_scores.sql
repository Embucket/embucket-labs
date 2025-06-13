WITH source AS (

    SELECT *
    FROM {{ ref('namespace_segmentation_scores_source') }}

)

SELECT *
FROM source
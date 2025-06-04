WITH source AS (

    SELECT *
    FROM {{ ref('ptpf_scores_source') }}

)

SELECT *
FROM source
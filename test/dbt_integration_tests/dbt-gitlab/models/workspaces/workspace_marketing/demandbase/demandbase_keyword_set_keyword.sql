WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_keyword_set_keyword_source') }}

)

SELECT *
FROM source
WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_keyword_set_source') }}

)

SELECT *
FROM source
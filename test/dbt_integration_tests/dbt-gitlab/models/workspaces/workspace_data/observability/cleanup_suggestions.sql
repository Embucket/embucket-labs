WITH source AS (

    SELECT *
    FROM {{ ref('cleanup_suggestions_source') }}

)

SELECT *
FROM source

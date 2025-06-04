WITH source AS (

    SELECT *
    FROM {{ ref('field_cleanup_suggestions_source') }}

)

SELECT *
FROM source

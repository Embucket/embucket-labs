WITH source AS (

    SELECT *
    FROM {{ ref('marketo_activity_type_source') }}

)

SELECT *
FROM source


WITH source AS (

    SELECT *
    FROM {{ ref('monitors_source') }}

)

SELECT *
FROM source

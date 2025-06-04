WITH source AS (

    SELECT *
    FROM {{ ref('xactly_planrules_source') }}

)

SELECT *
FROM source

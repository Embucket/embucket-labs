WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_departments_source') }}

)

SELECT *
FROM source
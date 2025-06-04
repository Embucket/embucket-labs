
WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sisense_users_source') }}

)

SELECT *
FROM source








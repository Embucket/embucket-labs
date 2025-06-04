WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_sisense_user_roles_source') }}

)

SELECT *
FROM source

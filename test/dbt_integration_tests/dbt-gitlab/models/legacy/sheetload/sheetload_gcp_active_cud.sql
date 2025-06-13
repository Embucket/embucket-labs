WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_gcp_active_cud_source') }}

)

SELECT *
FROM source
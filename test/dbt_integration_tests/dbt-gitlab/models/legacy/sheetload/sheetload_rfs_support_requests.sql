WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_rfs_support_requests_source') }}

)

SELECT *
FROM source


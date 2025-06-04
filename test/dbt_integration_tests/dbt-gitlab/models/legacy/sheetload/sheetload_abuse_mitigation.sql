WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_abuse_mitigation_source') }}

)

SELECT *
FROM source

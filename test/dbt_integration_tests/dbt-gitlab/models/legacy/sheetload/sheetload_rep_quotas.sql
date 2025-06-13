WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_rep_quotas_source') }}

)

SELECT *
FROM source
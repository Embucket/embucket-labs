WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_rep_quotas_full_ps_fy2020_source') }}

)

SELECT *
FROM source
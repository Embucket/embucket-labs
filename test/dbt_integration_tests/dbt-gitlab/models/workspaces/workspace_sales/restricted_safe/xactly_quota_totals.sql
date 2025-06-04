WITH source AS (

    SELECT *
    FROM {{ref('xactly_quota_totals_source')}}

)

SELECT *
FROM source
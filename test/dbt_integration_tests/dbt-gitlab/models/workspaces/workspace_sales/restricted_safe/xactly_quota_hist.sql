WITH source AS (

    SELECT *
    FROM {{ref('xactly_quota_hist_source')}}

)

SELECT *
FROM source
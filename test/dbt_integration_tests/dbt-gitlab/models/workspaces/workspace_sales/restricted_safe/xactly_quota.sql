WITH source AS (

    SELECT *
    FROM {{ref('xactly_quota_source')}}

)

SELECT *
FROM source
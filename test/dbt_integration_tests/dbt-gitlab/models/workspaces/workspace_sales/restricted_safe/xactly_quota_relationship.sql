WITH source AS (

    SELECT *
    FROM {{ref('xactly_quota_relationship_source')}}

)

SELECT *
FROM source
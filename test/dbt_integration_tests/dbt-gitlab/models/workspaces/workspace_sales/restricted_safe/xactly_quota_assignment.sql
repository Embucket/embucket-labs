WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_quota_assignment_source') }}
    FROM {{ ref('xactly_quota_assignment_source') }}

)

SELECT *
FROM source

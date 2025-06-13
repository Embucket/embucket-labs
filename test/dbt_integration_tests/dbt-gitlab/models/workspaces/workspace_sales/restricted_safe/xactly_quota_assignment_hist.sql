WITH source AS (

    SELECT {{ hash_sensitive_columns('xactly_quota_assignment_hist_source') }}
    FROM {{ ref('xactly_quota_assignment_hist_source') }}

)

SELECT *
FROM source

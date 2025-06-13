WITH source AS (

    SELECT
    {{ hash_sensitive_columns('sfdc_task_source') }}
    FROM {{ ref('sfdc_task_source') }}
    WHERE is_deleted = FALSE

)

SELECT *
FROM source


WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_crm_tasks_source') }}
    FROM {{ ref('bizible_crm_tasks_source') }}

)

SELECT *
FROM source
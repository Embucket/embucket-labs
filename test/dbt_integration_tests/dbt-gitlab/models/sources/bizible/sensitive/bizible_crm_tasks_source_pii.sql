WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_crm_tasks_source', 'crm_task_id') }}
    FROM {{ ref('bizible_crm_tasks_source') }}

)

SELECT *
FROM source
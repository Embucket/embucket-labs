WITH source AS (

    SELECT *
    FROM {{ ref('bizible_crm_tasks_source_pii') }}

)

SELECT *
FROM source
WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_marketing_core_users_from_docs_gitlab_com_source') }}

)

SELECT *
FROM source

{{ config({
    "schema": "legacy"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_ops_label_links_source') }}

)

SELECT *
FROM source

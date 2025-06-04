{{ config({
    "schema": "legacy",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_label_links_source') }}

)

SELECT *
FROM source

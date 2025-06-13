WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_page_views_source') }}
    FROM {{ ref('bizible_page_views_source') }}

)

SELECT *
FROM source
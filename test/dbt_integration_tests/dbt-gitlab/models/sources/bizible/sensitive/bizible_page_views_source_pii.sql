WITH source AS (

    SELECT {{ nohash_sensitive_columns('bizible_page_views_source', 'page_view_id') }}
    FROM {{ ref('bizible_page_views_source') }}

)

SELECT *
FROM source
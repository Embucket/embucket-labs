WITH source AS (

    SELECT *
    FROM {{ ref('bizible_page_views_source_pii') }}

)

SELECT *
FROM source
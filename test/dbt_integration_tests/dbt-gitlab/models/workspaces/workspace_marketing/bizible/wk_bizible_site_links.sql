WITH source AS (

    SELECT {{ hash_sensitive_columns('bizible_site_links_source') }}
    FROM {{ ref('bizible_site_links_source') }}

)

SELECT *
FROM source
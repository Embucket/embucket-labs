WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_vendors_source') }}

), entity_pii AS (

    SELECT
      vendor_id,
      {{ nohash_sensitive_columns('netsuite_vendors_source', 'vendor_name') }}
    FROM source

)

SELECT *
FROM entity_pii

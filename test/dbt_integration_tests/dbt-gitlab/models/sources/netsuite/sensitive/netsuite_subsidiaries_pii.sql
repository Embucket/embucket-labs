WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_subsidiaries_source') }}

), entity_pii AS (

    SELECT
      subsidiary_id,
      {{ nohash_sensitive_columns('netsuite_subsidiaries_source', 'subsidiary_name') }}
    FROM source

)

SELECT *
FROM entity_pii

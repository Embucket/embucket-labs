WITH source AS (

    SELECT *
    FROM {{ ref('netsuite_entity_source') }}

), entity_pii AS (

    SELECT
      entity_id,
      {{ nohash_sensitive_columns('netsuite_entity_source', 'entity_name') }}
    FROM source

)

SELECT *
FROM entity_pii

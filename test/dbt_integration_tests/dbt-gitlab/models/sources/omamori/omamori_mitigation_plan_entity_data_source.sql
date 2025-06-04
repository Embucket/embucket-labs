{{ omamori_incremental_source('mitigation_plan_entity_data_external') }}

renamed AS (
  SELECT
    json_value['id']::INT                                          AS id,
    json_value['status']::VARCHAR                                  AS status,
    json_value['entity_datum_id']::INT                             AS entity_datum_id,
    json_value['mitigation_plan_id']::INT                          AS mitigation_plan_id,
    -- convert epoch microseconds to Snowflake timestamp
    (json_value['created_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS created_at,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped









WITH source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at_gcs' from the parquet filename, i.e:
    -- 'entity_data/20230502/20230502T000025-entity_data-from-19700101T000000-until-20230426T193740.parquet'
    TO_TIMESTAMP('20240417T123000', 'YYYYMMDDTHH24MISS') AS uploaded_at_gcs
  FROM
    RAW.omamori.mitigation_plan_template_tags_external

  
),

renamed AS (
  SELECT
    json_value['id']::INT                                          AS id,
    (json_value['created_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS created_at,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS updated_at,
    json_value['mitigation_plan_template_id']::INT                 AS mitigation_plan_template_id,
    json_value['tag_id']::INT                                      AS tag_id,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped
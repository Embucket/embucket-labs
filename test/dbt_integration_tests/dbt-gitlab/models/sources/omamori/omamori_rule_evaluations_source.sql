{{ omamori_incremental_source('rule_evaluations_external') }}

renamed AS (
  SELECT
    json_value['id']::INT                                          AS id,
    json_value['rule']::VARCHAR                                    AS rule,
    json_value['result']::VARCHAR                                  AS outcome,
    json_value['elapsed_ms']::INT                                  AS elapsed_ms,
    json_value['throttled_count']::INT                             AS throttled_count,
    json_value['duplicates_removed_count']::INT                    AS duplicates_removed_count,
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

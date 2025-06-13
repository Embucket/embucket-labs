{{ omamori_incremental_source('mitigation_plans_external') }}

renamed AS (
  SELECT
    json_value['id']::INT                                           AS id,
    json_value['entity_type']::VARCHAR                              AS entity_type,
    json_value['priority']::VARCHAR                                 AS priority,
    json_value['status']::VARCHAR                                   AS status,
    json_value['feature']::VARCHAR                                  AS feature,
    json_value['category']::VARCHAR                                 AS category,
    json_value['last_applied_mitigation_plan_template_id']::INT     AS last_applied_mitigation_plan_template_id,
    json_value['from_rule_evaluation_id']::INT                      AS from_rule_evaluation_id,
    (json_value['created_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP  AS created_at,
    json_value['created_by_id']::INT                                AS created_by_id,
    (json_value['reviewed_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS reviwed_at,
    json_value['reviewed_by_id']::INT                               AS reviewed_by_id,
    (json_value['executed_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS executed_at,
    json_value['executed_by_id']::INT                               AS executed_by_id,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP  AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped

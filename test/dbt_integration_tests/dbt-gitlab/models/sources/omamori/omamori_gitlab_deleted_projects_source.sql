{{ omamori_incremental_source('gitlab_deleted_projects_external') }}

renamed AS (
  SELECT
    json_value['id']::INT                                          AS id,
    (json_value['deleted_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS deleted_at,
    (json_value['updated_at']::NUMBER(36, 3) / 1000000)::TIMESTAMP AS updated_at,
    uploaded_at_gcs
  FROM source
),

dedupped AS (
  SELECT * FROM renamed
  QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
)

SELECT * FROM dedupped

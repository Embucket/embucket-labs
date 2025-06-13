{{ omamori_incremental_source('users_external') }}

renamed AS (
  SELECT


    json_value['id']::INT                                          AS id,
    json_value['username']::VARCHAR                                AS username,
    json_value['name']::VARCHAR                                    AS full_name,
    json_value['role']::VARCHAR                                    AS user_role,
    json_value['service_user']::BOOLEAN                            AS service_user,

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

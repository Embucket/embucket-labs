{{ level_up_intermediate('clients') }}

parsed AS (
  SELECT
    value['createdAt']::TIMESTAMP AS created_at,
    value['disabled']::BOOLEAN    AS is_disabled,
    value['id']::VARCHAR          AS client_id,
    value['name']::VARCHAR        AS name,
    value['sku']::VARCHAR         AS sku,
    value['slug']::VARCHAR        AS slug,
    value['tags']::VARIANT        AS tags,
    value['updatedAt']::TIMESTAMP AS updated_at,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        client_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed

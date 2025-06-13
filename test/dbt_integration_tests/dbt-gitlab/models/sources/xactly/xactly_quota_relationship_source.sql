WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota_relationship') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT              AS created_by_id,
      created_date::VARCHAR             AS created_date,
      description::VARCHAR              AS description,
      is_active::VARCHAR                AS is_active,
      label::VARCHAR                    AS label,
      modified_by_id::FLOAT             AS modified_by_id,
      modified_date::VARCHAR            AS modified_date,
      name::VARCHAR                     AS name,
      quota_relationship_id::VARCHAR    AS quota_relationship_id

    FROM source
    
)

SELECT *
FROM renamed
WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      from_pos_id::FLOAT                    AS from_pos_id,
      from_pos_name::VARCHAR                AS from_pos_name,
      is_active::VARCHAR                    AS is_active,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      pos_hierarchy_id::FLOAT               AS pos_hierarchy_id,
      pos_hierarchy_type_id::FLOAT          AS pos_hierarchy_type_id,
      to_pos_id::FLOAT                      AS to_pos_id,
      to_pos_name::VARCHAR                  AS to_pos_name,
      version::FLOAT                        AS version

    FROM source
    
)

SELECT *
FROM renamed
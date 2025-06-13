WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_relations_hist') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      from_pos_id::FLOAT                    AS from_pos_id,
      from_pos_name::VARCHAR                AS from_pos_name,
      id::FLOAT                             AS id,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      object_id::FLOAT                      AS object_id,
      pos_rel_type_id::FLOAT                AS pos_rel_type_id,
      to_pos_id::FLOAT                      AS to_pos_id,
      to_pos_name::VARCHAR                  AS to_pos_name

    FROM source
    
)

SELECT *
FROM renamed
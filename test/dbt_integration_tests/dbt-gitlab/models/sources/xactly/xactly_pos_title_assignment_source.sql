WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_title_assignment') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      is_active::VARCHAR                    AS is_active,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      pos_title_assignment_id::FLOAT        AS pos_title_assignment_id,
      position_id::FLOAT                    AS position_id,
      position_name::VARCHAR                AS position_name,
      title_id::FLOAT                       AS title_id,
      title_name::VARCHAR AS title_name

    FROM source
    
)

SELECT *
FROM renamed
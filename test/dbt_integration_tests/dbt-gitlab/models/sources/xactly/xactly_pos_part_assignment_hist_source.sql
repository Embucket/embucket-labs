WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_part_assignment_hist') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      is_active::VARCHAR                    AS is_active,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      object_id::FLOAT                      AS object_id,
      participant_id::FLOAT                 AS participant_id,
      participant_name::VARCHAR             AS participant_name,
      pos_part_assignment_id::FLOAT         AS pos_part_assignment_id,
      position_id::FLOAT                    AS position_id,
      position_name::VARCHAR                AS position_name

    FROM source
    
)

SELECT *
FROM renamed
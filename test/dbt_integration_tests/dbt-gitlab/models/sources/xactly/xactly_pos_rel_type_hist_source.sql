WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_rel_type_hist') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                      AS created_by_id,
      created_by_name::VARCHAR                  AS created_by_name,
      created_date::VARCHAR                     AS created_date,
      descr::VARCHAR                            AS descr,
      effective_end_date::VARCHAR               AS effective_end_date,
      effective_start_date::VARCHAR             AS effective_start_date,
      is_active::VARCHAR                        AS is_active,
      is_master::VARCHAR                        AS is_master,
      modified_by_id::FLOAT                     AS modified_by_id,
      modified_by_name::VARCHAR                 AS modified_by_name,
      modified_date::VARCHAR                    AS modified_date,
      name::VARCHAR                             AS name,
      object_id::FLOAT                          AS object_id,
      pos_rel_type_id::FLOAT                    AS pos_rel_type_id

    FROM source
    
)

SELECT *
FROM renamed
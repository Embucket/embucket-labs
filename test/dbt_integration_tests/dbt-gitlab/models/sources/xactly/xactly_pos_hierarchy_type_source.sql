WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_pos_hierarchy_type') }}

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
      pos_hierarchy_type_disp_name::VARCHAR     AS pos_hierarchy_type_disp_name,
      pos_hierarchy_type_id::FLOAT              AS pos_hierarchy_type_id,
      pos_hierarchy_type_name::VARCHAR          AS pos_hierarchy_type_name

    FROM source
    
)

SELECT *
FROM renamed
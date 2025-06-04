WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_position') }}

), renamed AS (

    SELECT

      business_group_id::FLOAT              AS business_group_id,
      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      credit_end_date::VARCHAR              AS credit_end_date,
      credit_start_date::VARCHAR            AS credit_start_date,
      descr::VARCHAR                        AS descr,
      effective_end_date::VARCHAR           AS effective_end_date,
      effective_start_date::VARCHAR         AS effective_start_date,
      incent_end_date::VARCHAR              AS incent_end_date,
      incent_st_date::VARCHAR               AS incent_st_date,
      is_active::VARCHAR                    AS is_active,
      is_master::VARCHAR                    AS is_master,
      master_position_id::FLOAT             AS master_position_id,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      name::VARCHAR                         AS name,
      parent_position_id::FLOAT             AS parent_position_id,
      parent_record_id::FLOAT               AS parent_record_id,
      participant_id::FLOAT                 AS participant_id,
      pos_group_id::FLOAT                   AS pos_group_id,
      position_id::FLOAT                    AS position_id,
      title_id::FLOAT                       AS title_id

    FROM source
    
)

SELECT *
FROM renamed
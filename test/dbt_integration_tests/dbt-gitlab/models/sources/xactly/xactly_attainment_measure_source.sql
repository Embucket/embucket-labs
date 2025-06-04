WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_attainment_measure') }}

), renamed AS (

    SELECT

      attainment_measure_id::VARCHAR                AS attainment_measure_id,
      created_by_id::FLOAT                          AS created_by_id,
      created_date::VARCHAR                         AS created_date,
      description::VARCHAR                          AS description,
      effective_end_period_id::FLOAT                AS effective_end_period_id,
      effective_start_period_id::FLOAT              AS effective_start_period_id,
      history_uuid::VARCHAR                         AS history_uuid,
      is_active::VARCHAR                            AS is_active,
      master_attainment_measure_id::VARCHAR         AS master_attainment_measure_id,
      modified_by_id::FLOAT                         AS modified_by_id,
      modified_date::VARCHAR                        AS modified_date,
      name::VARCHAR                                 AS name,
      period_type::VARCHAR                          AS period_type

    FROM source
    
)

SELECT *
FROM renamed
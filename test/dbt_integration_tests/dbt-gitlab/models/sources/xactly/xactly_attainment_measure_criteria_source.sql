WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_attainment_measure_criteria') }}

), renamed AS (

    SELECT

      attainment_measure_criteria_id::VARCHAR               AS attainment_measure_criteria_id,
      attainment_measure_id::VARCHAR                        AS attainment_measure_id,
      created_by_id::FLOAT                                  AS created_by_id,
      created_date::VARCHAR                                 AS created_date,
      criteria_id::FLOAT                                    AS criteria_id,
      history_uuid::VARCHAR                                 AS history_uuid,
      is_active::VARCHAR                                    AS is_active,
      modified_by_id::FLOAT                                 AS modified_by_id,
      modified_date::VARCHAR                                AS modified_date,
      name::VARCHAR                                         AS name,
      type::VARCHAR                                         AS type

    FROM source
)

SELECT *
FROM renamed
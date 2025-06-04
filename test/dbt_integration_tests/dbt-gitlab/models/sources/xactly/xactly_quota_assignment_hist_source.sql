WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota_assignment_hist') }}

), renamed AS (

    SELECT

      amount::FLOAT                         AS amount,
      amount_unit_type_id::FLOAT            AS amount_unit_type_id,
      assignment_id::FLOAT                  AS assignment_id,
      assignment_name::VARCHAR              AS assignment_name,
      assignment_type::FLOAT                AS assignment_type,
      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      description::VARCHAR                  AS description,
      effective_end_period_id::FLOAT        AS effective_end_period_id,
      effective_start_period_id::FLOAT      AS effective_start_period_id,
      is_active::VARCHAR                    AS is_active,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      object_id::FLOAT                      AS object_id,
      period_id::FLOAT                      AS period_id,
      quota_assignment_id::FLOAT            AS quota_assignment_id,
      quota_id::FLOAT                       AS quota_id

    FROM source

)

SELECT *
FROM renamed
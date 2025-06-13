WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_adjustment') }}

), renamed AS (

    SELECT

      amount::FLOAT                         AS amount,
      amount_unit_type_id::FLOAT            AS amount_unit_type_id,
      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      credit_adjustment_id::FLOAT           AS credit_adjustment_id,
      credit_id::FLOAT                      AS credit_id,
      is_active::VARCHAR                    AS is_active,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      period_id::FLOAT                      AS period_id,
      reason_id::FLOAT                      AS reason_id

    FROM source
    
)

SELECT *
FROM renamed
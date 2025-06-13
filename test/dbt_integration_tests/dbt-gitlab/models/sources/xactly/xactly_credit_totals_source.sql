WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_totals') }}

), renamed AS (

    SELECT

      amount::FLOAT                     AS amount,
      created_by_id::FLOAT              AS created_by_id,
      created_by_name::VARCHAR          AS created_by_name,
      created_date::VARCHAR             AS created_date,
      credit_totals_id::FLOAT           AS credit_totals_id,
      is_active::VARCHAR                AS is_active,
      modified_by_id::FLOAT             AS modified_by_id,
      modified_by_name::VARCHAR         AS modified_by_name,
      modified_date::VARCHAR            AS modified_date,
      participant_id::FLOAT             AS participant_id,
      participant_name::VARCHAR         AS participant_name,
      period_id::FLOAT                  AS period_id,
      position_id::FLOAT                AS position_id,
      position_name::VARCHAR            AS position_name,
      unittype_id::FLOAT                AS unittype_id

    FROM source
    
)

SELECT *
FROM renamed
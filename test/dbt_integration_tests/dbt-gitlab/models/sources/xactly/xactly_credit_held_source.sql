WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_credit_held') }}

), renamed AS (

    SELECT

      created_by_id::FLOAT                  AS created_by_id,
      created_by_name::VARCHAR              AS created_by_name,
      created_date::VARCHAR                 AS created_date,
      credit_held_id::FLOAT                 AS credit_held_id,
      credit_id::FLOAT                      AS credit_id,
      held_date::VARCHAR                    AS held_date,
      is_active::VARCHAR                    AS is_active,
      is_held::VARCHAR                      AS is_held,
      modified_by_id::FLOAT                 AS modified_by_id,
      modified_by_name::VARCHAR             AS modified_by_name,
      modified_date::VARCHAR                AS modified_date,
      release_group_id::FLOAT               AS release_group_id,
      run_id::FLOAT                         AS run_id,
      trans_id::FLOAT                       AS trans_id

    FROM source
    
)

SELECT *
FROM renamed
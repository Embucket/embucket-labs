WITH source AS (

    SELECT *
    FROM {{ source('xactly', 'xc_quota') }}

), renamed AS (

    SELECT

      classification_name::VARCHAR                  AS classification_name,
      classification_type::FLOAT                    AS classification_type,
      created_by_id::FLOAT                          AS created_by_id,
      created_by_name::VARCHAR                      AS created_by_name,
      created_date::VARCHAR                         AS created_date,
      description::VARCHAR                          AS description,
      is_active::VARCHAR                            AS is_active,
      modified_by_id::FLOAT                         AS modified_by_id,
      modified_by_name::VARCHAR                     AS modified_by_name,
      modified_date::VARCHAR                        AS modified_date,
      name::VARCHAR                                 AS name,
      quota_id::FLOAT                               AS quota_id,
      quota_interval_id::FLOAT                      AS quota_interval_id,
      quota_interval_name::VARCHAR                  AS quota_interval_name,
      quota_period_id::FLOAT                        AS quota_period_id,
      quota_period_name::VARCHAR                    AS quota_period_name,
      quota_value::FLOAT                            AS quota_value,
      quota_value_unit_type_id::FLOAT               AS quota_value_unit_type_id,
      quota_value_unit_type_name::VARCHAR           AS quota_value_unit_type_name,
      source_id::FLOAT                              AS source_id

    FROM source
    
)

SELECT *
FROM renamed
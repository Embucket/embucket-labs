{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_quote_history') }}

), renamed AS (

  SELECT
    createdbyid::VARCHAR                                   AS created_by_id,
    createddate::TIMESTAMP                                 AS created_date,
    datatype::VARCHAR                                      AS data_type,
    field::VARCHAR                                         AS changed_field,
    id::VARCHAR                                            AS quote_change_id,
    parentid::VARCHAR                                      AS zqu_quote_id,
    oldvalue__bo::BOOLEAN                                  AS old_value_bo,
    oldvalue__de::DECIMAL                                  AS old_value_de,
    oldvalue__fl::FLOAT                                    AS old_value_fl,
    oldvalue__st::VARCHAR                                  AS old_value_st,
    newvalue__bo::BOOLEAN                                  AS new_value_bo,
    newvalue__de::DECIMAL                                  AS new_value_de,
    newvalue__fl::FLOAT                                    AS new_value_fl,
    newvalue__st::VARCHAR                                  AS new_value_st,
    isdeleted::BOOLEAN                                     AS is_deleted,
    _sdc_extracted_at::TIMESTAMP_TZ                        AS sdc_extracted_at,
    _sdc_received_at::TIMESTAMP_TZ                         AS sdc_received_at,
    _sdc_batched_at::TIMESTAMP_TZ                          AS sdc_batched_at,
    _sdc_sequence::NUMBER                                  AS sdc_sequence,
    _sdc_table_version::NUMBER                             AS sdc_table_version
  FROM source
)

SELECT *
FROM renamed

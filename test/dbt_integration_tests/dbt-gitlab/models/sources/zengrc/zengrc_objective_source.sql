WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'objectives') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                          AS objective_code,
      created_at::TIMESTAMP                                  AS objective_created_at,
      description::VARCHAR                                   AS objective_description,
      id::NUMBER                                             AS objective_id,
      os_state::VARCHAR                                      AS objective_os_state,
      status::VARCHAR                                        AS objective_status,
      title::VARCHAR                                         AS objective_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS objective_updated_at,
      _sdc_extracted_at::TIMESTAMP                           AS objective_loaded_at,
      PARSE_JSON(custom_attributes)['3']['value']::VARCHAR   AS fedramp_parameter,
      PARSE_JSON(custom_attributes)['219']['value']::VARCHAR AS security_requirement_nist_800_171

    FROM source

)

SELECT *
FROM renamed




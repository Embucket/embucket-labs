WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'controls') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                          AS control_code,
      created_at::TIMESTAMP                                  AS control_created_at,
      description::VARCHAR                                   AS control_description,
      id::NUMBER                                             AS control_id,
      mapped['objectives']::VARIANT                          AS mapped_objectives,
      status::VARCHAR                                        AS control_status,
      title::VARCHAR                                         AS control_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS control_updated_at,
      _sdc_extracted_at::TIMESTAMP                           AS control_loaded_at,
      PARSE_JSON(custom_attributes)['187']['value']::VARCHAR AS application_used,
      PARSE_JSON(custom_attributes)['106']['value']::VARCHAR AS average_control_effectiveness_rating,
      PARSE_JSON(custom_attributes)['107']['value']::VARCHAR AS control_deployment,
      PARSE_JSON(custom_attributes)['116']['value']::VARCHAR AS control_level,
      PARSE_JSON(custom_attributes)['182']['value']::VARCHAR AS control_objective,
      PARSE_JSON(custom_attributes)['181']['value']::VARCHAR AS control_objective_ref_number,
      PARSE_JSON(custom_attributes)['186']['value']::VARCHAR AS control_type,
      PARSE_JSON(custom_attributes)['214']['value']::VARCHAR AS coso_components,
      PARSE_JSON(custom_attributes)['215']['value']::VARCHAR AS coso_principles,
      PARSE_JSON(custom_attributes)['174']['value']::VARCHAR AS financial_cycle,
      PARSE_JSON(custom_attributes)['213']['value']::VARCHAR AS financial_statement_assertions,
      PARSE_JSON(custom_attributes)['117']['value']::VARCHAR AS gitlab_guidance,
      PARSE_JSON(custom_attributes)['205']['value']::VARCHAR AS ia_significance,
      PARSE_JSON(custom_attributes)['196']['value']::VARCHAR AS ia_test_plan,
      PARSE_JSON(custom_attributes)['192']['value']::VARCHAR AS ipe_report_name,
      PARSE_JSON(custom_attributes)['193']['value']::VARCHAR AS ipe_review_parameters,
      PARSE_JSON(custom_attributes)['188']['value']::VARCHAR AS management_review_control,
      PARSE_JSON(custom_attributes)['183']['value']::VARCHAR AS mitigating_control_activities,
      PARSE_JSON(custom_attributes)['112']['value']::VARCHAR AS original_scf_control_language,
      PARSE_JSON(custom_attributes)['175']['value']::VARCHAR AS process,
      PARSE_JSON(custom_attributes)['184']['value']::VARCHAR AS process_owner_name,
      PARSE_JSON(custom_attributes)['185']['value']::VARCHAR AS process_owner_title,
      PARSE_JSON(custom_attributes)['12']['value']::NUMBER   AS relative_control_weighting,
      PARSE_JSON(custom_attributes)['179']['value']::VARCHAR AS risk_event,
      PARSE_JSON(custom_attributes)['178']['value']::VARCHAR AS risk_ref_number,
      PARSE_JSON(custom_attributes)['10']['value']::VARCHAR  AS sample_control_implementation,
      PARSE_JSON(custom_attributes)['11']['value']::VARCHAR  AS scf_control_question,
      PARSE_JSON(custom_attributes)['194']['value']::VARCHAR AS spreadsheet_name,
      PARSE_JSON(custom_attributes)['195']['value']::VARCHAR AS spreadsheet_review_parameters,
      PARSE_JSON(custom_attributes)['176']['value']::VARCHAR AS sub_process

    FROM source

)

SELECT *
FROM renamed




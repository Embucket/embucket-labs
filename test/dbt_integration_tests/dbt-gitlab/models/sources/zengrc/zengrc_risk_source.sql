WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'risks') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                          AS risk_code,
      created_at::TIMESTAMP                                  AS risk_created_at,
      custom_attributes::VARIANT                             AS risk_custom_attributes,
      description::VARCHAR                                   AS risk_description,
      id::NUMBER                                             AS risk_id,
      risk_vector_score_values::VARIANT                      AS risk_vector_score_values,
      status::VARCHAR                                        AS risk_status,
      title::VARCHAR                                         AS risk_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS risk_updated_at,
      _sdc_extracted_at::TIMESTAMP                           AS risk_loaded_at,
      PARSE_JSON(custom_attributes)['156']['value']::VARCHAR AS acceptance_of_risk_ownership,
      PARSE_JSON(custom_attributes)['44']['value']::VARCHAR  AS cia_impact,
      PARSE_JSON(custom_attributes)['79']['value']::DATE     AS risk_identified_date,
      PARSE_JSON(custom_attributes)['81']['value']::VARCHAR  AS existing_mitigations,
      PARSE_JSON(custom_attributes)['57']['value']::VARCHAR  AS interested_parties,
      PARSE_JSON(custom_attributes)['158']['value']::BOOLEAN AS is_risk_ready_for_review_and_closure,
      PARSE_JSON(custom_attributes)['46']['value']::VARCHAR  AS risk_owner,
      PARSE_JSON(custom_attributes)['74']['value']::VARCHAR  AS risk_tier,
      PARSE_JSON(custom_attributes)['160']['value']::DATE    AS risk_treatment_completion_date,
      PARSE_JSON(custom_attributes)['159']['value']::VARCHAR AS risk_treatment_option_selected,
      PARSE_JSON(custom_attributes)['45']['value']::VARCHAR  AS root_cause,
      PARSE_JSON(custom_attributes)['58']['value']::VARCHAR  AS threat_source,
      PARSE_JSON(custom_attributes)['148']['value']::BOOLEAN AS is_tprm_related,
      PARSE_JSON(custom_attributes)['75']['value']::BOOLEAN  AS is_within_risk_appetite

    FROM source

)

SELECT *
FROM renamed
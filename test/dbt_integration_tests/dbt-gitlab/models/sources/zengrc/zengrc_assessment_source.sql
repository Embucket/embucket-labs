WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'assessments') }}

),

renamed AS (

    SELECT
      assessors::VARIANT                                     AS assessors,
      code::VARCHAR                                          AS assessment_code,
      conclusion::VARCHAR                                    AS assessment_conclusion,
      control['id']::NUMBER                                  AS control_id,
      control['title']::VARCHAR                              AS control_title,
      control['type']::VARCHAR                               AS control_type,
      created_at::TIMESTAMP                                  AS assessment_created_at,
      description::VARCHAR                                   AS assessment_description,
      end_date::DATE                                         AS assessment_end_date,
      id::NUMBER                                             AS assessment_id,
      mapped['audits']::VARIANT                              AS mapped_audits,
      start_date::DATE                                       AS assessment_start_date,
      status::VARCHAR                                        AS assessment_status,
      title::VARCHAR                                         AS assessment_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS assessment_uploaded_at,
      _sdc_extracted_at::TIMESTAMP                           AS assessment_loaded_at,
      PARSE_JSON(custom_attributes)['141']['value']::VARCHAR AS annualized_population_size,
      PARSE_JSON(custom_attributes)['169']['value']::VARCHAR AS competency_and_authority_of_control_owners,
      PARSE_JSON(custom_attributes)['125']['value']::VARCHAR AS control_health_and_effectiveness_rating_cher,
      PARSE_JSON(custom_attributes)['135']['value']::VARCHAR AS control_implementation_statement,
      PARSE_JSON(custom_attributes)['172']['value']::VARCHAR AS criteria_for_investigation_and_follow_up,
      PARSE_JSON(custom_attributes)['134']['value']::VARCHAR AS customer_hosting_option,
      PARSE_JSON(custom_attributes)['165']['value']::VARCHAR AS gitlab_control_family,
      PARSE_JSON(custom_attributes)['113']['value']::VARCHAR AS gitlab_control_owner,
      PARSE_JSON(custom_attributes)['171']['value']::VARCHAR AS level_of_consistency_and_frequency,
      PARSE_JSON(custom_attributes)['170']['value']::VARCHAR AS level_of_judgement_and_aggregation,
      PARSE_JSON(custom_attributes)['39']['value']::VARCHAR  AS population_date_range,
      PARSE_JSON(custom_attributes)['140']['value']::VARCHAR AS population_size,
      PARSE_JSON(custom_attributes)['167']['value']::VARCHAR AS purpose_and_appropriateness_of_the_control,
      PARSE_JSON(custom_attributes)['168']['value']::VARCHAR AS risk_associated_with_the_control,
      PARSE_JSON(custom_attributes)['139']['value']::VARCHAR AS sample_size,
      PARSE_JSON(custom_attributes)['118']['value']::VARCHAR AS system_level,
      PARSE_JSON(custom_attributes)['104']['value']::VARCHAR AS test_of_design_results,
      PARSE_JSON(custom_attributes)['105']['value']::VARCHAR AS test_of_operating_effectiveness_results,
      PARSE_JSON(custom_attributes)['128']['value']::NUMBER  AS total_hours_estimate

    FROM source

)

SELECT *
FROM renamed




WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'audits') }}

),

renamed AS (

    SELECT
      audit_managers::VARIANT                                AS audit_managers,
      code::VARCHAR                                          AS audit_code,
      created_at::TIMESTAMP                                  AS audit_created_at,
      description::VARCHAR                                   AS audidt_description,
      end_date::DATE                                         AS audit_end_date,
      id::NUMBER                                             AS audit_id,
      mapped['markets']::VARIANT                             AS mapped_markets,
      mapped['standards']::VARIANT                           AS mapped_standards,
      program['id']::NUMBER                                  AS program_id,
      program['title']::VARCHAR                              AS program_title,
      program['type']::VARCHAR                               AS program_type,
      report_period_end_date::DATE                           AS audit_report_period_end_date,
      report_period_start_date::DATE                         AS audit_report_period_start_date,
      start_date::DATE                                       AS audit_start_date,
      status::VARCHAR                                        AS audit_status,
      sync_external_attachments::BOOLEAN                     AS has_external_attachments,
      sync_external_comments::BOOLEAN                        AS has_external_comments,
      title::VARCHAR                                         AS audit_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS audit_uploaded_at,
      _sdc_extracted_at::TIMESTAMP                                 AS audit_loaded_at,
      PARSE_JSON(custom_attributes)['66']['value']::VARCHAR  AS audit_category,
      PARSE_JSON(custom_attributes)['212']['value']::DATE    AS audit_completion_date,
      PARSE_JSON(custom_attributes)['206']['value']::VARCHAR AS delegated_testing_owner,
      PARSE_JSON(custom_attributes)['216']['value']::DATE    AS documentation_due_date,
      PARSE_JSON(custom_attributes)['217']['value']::DATE    AS escalation_date,
      PARSE_JSON(custom_attributes)['123']['value']::VARCHAR AS gitlab_assignee,
      PARSE_JSON(custom_attributes)['149']['value']::VARCHAR AS inherent_risk,
      PARSE_JSON(custom_attributes)['152']['value']::VARCHAR AS period_completed,
      PARSE_JSON(custom_attributes)['151']['value']::VARCHAR AS period_created,
      PARSE_JSON(custom_attributes)['150']['value']::VARCHAR AS residual_risk,
      PARSE_JSON(custom_attributes)['147']['value']::VARCHAR AS system_effectiveness_rating,
      PARSE_JSON(custom_attributes)['121']['value']::VARCHAR AS system_tier_level

    FROM source

)

SELECT *
FROM renamed




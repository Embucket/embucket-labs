WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'issues') }}

),

renamed AS (

    SELECT
      code::VARCHAR                                                             AS issue_code,
      created_at::TIMESTAMP                                                     AS issue_created_at,
      custom_attributes::VARIANT                                                AS issue_custom_attributes,
      description::VARCHAR                                                      AS issue_description,
      id::NUMBER                                                                AS issue_id,
      mapped['audits']::VARIANT                                                 AS mapped_audits,
      mapped['controls']::VARIANT                                               AS mapped_controls,
      mapped['programs']::VARIANT                                               AS mapped_programs,
      mapped['standards']::VARIANT                                              AS mapped_standards,
      notes::VARCHAR                                                            AS issue_notes,
      status::VARCHAR                                                           AS issue_status,
      stop_date::DATE                                                           AS issue_stop_date,
      tags::VARCHAR                                                             AS issue_tags,
      title::VARCHAR                                                            AS issue_title,
      type::VARCHAR                                                             AS zengrc_object_type,
      updated_at::TIMESTAMP                                                     AS issue_updated_at,
      _sdc_extracted_at::TIMESTAMP                                              AS issue_loaded_at,
      PARSE_JSON(issue_custom_attributes)['109']['value']::VARCHAR              AS remediation_recommendations,
      PARSE_JSON(issue_custom_attributes)['110']['value']::VARCHAR              AS deficiency_range,
      PARSE_JSON(issue_custom_attributes)['111']['value']::VARCHAR              AS risk_rating,
      PARSE_JSON(issue_custom_attributes)['115']['value']::VARCHAR              AS observation_issue_owner,
      PARSE_JSON(issue_custom_attributes)['142']['value']::NUMBER               AS likelihood,
      PARSE_JSON(issue_custom_attributes)['143']['value']::NUMBER               AS impact,
      PARSE_JSON(issue_custom_attributes)['153']['value']::VARCHAR              AS gitlab_issue_url,
      PARSE_JSON(issue_custom_attributes)['154']['value']::VARCHAR              AS gitlab_assignee,
      PARSE_JSON(issue_custom_attributes)['155']['value']::VARCHAR              AS department,
      PARSE_JSON(issue_custom_attributes)['197']['value']::VARCHAR              AS type_of_deficiency,
      PARSE_JSON(issue_custom_attributes)['199']['value']::VARCHAR              AS internal_control_component,
      PARSE_JSON(issue_custom_attributes)['200']['value']::VARCHAR              AS severity_of_deficiency,
      PARSE_JSON(issue_custom_attributes)['202']['value']::VARCHAR              AS financial_system_line_item,
      PARSE_JSON(issue_custom_attributes)['207']['value']::BOOLEAN              AS is_reported_to_audit_committee,
      PARSE_JSON(issue_custom_attributes)['203']['value']::VARCHAR              AS deficiency_theme,
      PARSE_JSON(issue_custom_attributes)['204']['value']::VARCHAR              AS remediated_evidence,
      PARSE_JSON(issue_custom_attributes)['198']['value']::VARCHAR              AS financial_assertion_affected_by_deficiency,
      PARSE_JSON(issue_custom_attributes)['201']['value']::VARCHAR              AS compensating_controls,
      REGEXP_SUBSTR(gitlab_issue_url,'\/issues\/([0-9]+)[^0-9]*',1,1,'e',1)     AS gitlab_dotcom_issue_id
    FROM source

)

SELECT *
FROM renamed

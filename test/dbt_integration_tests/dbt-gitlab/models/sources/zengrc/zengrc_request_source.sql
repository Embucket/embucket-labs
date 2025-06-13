WITH source AS (

    SELECT *
    FROM {{ source('zengrc', 'requests') }}

),

renamed AS (

    SELECT
      assignees::VARIANT                                     AS assignees,
      audit['id']::NUMBER                                    AS audit_id,
      audit['title']::VARCHAR                                AS audit_title,
      code::VARCHAR                                          AS request_code,
      created_at::TIMESTAMP                                  AS request_created_at,
      custom_attributes::VARIANT                             AS request_custom_attributes,
      description::VARCHAR                                   AS request_description,
      end_date::DATE                                         AS request_end_date,
      id::NUMBER                                             AS request_id,
      mapped['controls']::VARIANT                            AS mapped_controls,
      mapped['issues']::VARIANT                              AS mapped_issues,
      mapped['programs']::VARIANT                            AS mapped_programs,
      requesters::VARIANT                                    AS requestors,
      start_date::DATE                                       AS request_start_date,
      status::VARCHAR                                        AS request_status,
      stop_date::DATE                                        AS request_stop_date,
      tags::VARCHAR                                          AS request_tags,
      title::VARCHAR                                         AS request_title,
      type::VARCHAR                                          AS zengrc_object_type,
      updated_at::TIMESTAMP                                  AS request_updated_at,
      _sdc_extracted_at::TIMESTAMP                           AS request_loaded_at,
      PARSE_JSON(custom_attributes)['209']['value']::VARCHAR AS arr_impact,
      PARSE_JSON(custom_attributes)['68']['value']::VARCHAR  AS audit_period,
      PARSE_JSON(custom_attributes)['173']['value']::VARCHAR AS caa_activity_type,
      PARSE_JSON(custom_attributes)['70']['value']::BOOLEAN  AS is_external_audit,
      PARSE_JSON(custom_attributes)['59']['value']::VARCHAR  AS gitlab_assignee,
      PARSE_JSON(custom_attributes)['60']['value']::VARCHAR  AS gitlab_issue_url,
      PARSE_JSON(custom_attributes)['69']['value']::VARCHAR  AS priority_level

    FROM source

)

SELECT *
FROM renamed
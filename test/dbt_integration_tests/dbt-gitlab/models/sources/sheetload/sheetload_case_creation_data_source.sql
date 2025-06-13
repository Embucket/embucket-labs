WITH source AS (

  SELECT *
  FROM {{ source('sheetload','case_creation_data') }}

),

renamed AS (

  SELECT
    TRY_TO_NUMBER(case_trigger_id)                   AS case_trigger_id,
    case_trigger ::VARCHAR                           AS case_trigger,
    owner_id::VARCHAR                                AS owner_id,
    status::VARCHAR                                  AS status,
    case_origin::VARCHAR                             AS case_origin,
    type::VARCHAR                                    AS type,
    case_subject::VARCHAR                            AS case_subject,
    case_reason::VARCHAR                             AS case_reason,
    record_type_id::VARCHAR                          AS record_type_id,
    priority::VARCHAR                                AS priority,
    live::VARCHAR                                    AS live,
    case_cta::VARCHAR                                AS case_cta,
    case_context::VARCHAR                            AS case_context
  FROM source

)

SELECT *
FROM renamed

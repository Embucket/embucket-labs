WITH base AS (

    SELECT *
    FROM {{ source('salesforce', 'casehistory') }}

), renamed AS (

    SELECT
      caseid::VARCHAR                AS case_id,
      id::VARCHAR                    AS case_history_id,
      createddate::DATE           AS field_modified_at,
      LOWER(field)::VARCHAR          AS case_field,
      LOWER(datatype)::VARCHAR       AS data_type,
      newvalue::VARCHAR              AS new_value,
      oldvalue::VARCHAR              AS old_value,
      isdeleted::BOOLEAN             AS is_deleted,
      createdbyid::VARCHAR           AS created_by_id
    FROM base

)

SELECT *
FROM renamed
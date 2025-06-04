WITH source AS (

  SELECT *
  FROM {{ source('workday','assess_talent') }}
  WHERE NOT _fivetran_deleted

),

renamed AS (

  SELECT
    source.employee_id::NUMBER            AS employee_id,
    d.value['WORKDAY_ID']::VARCHAR        AS workday_id,
    d.value['KEY_TALENT']::VARCHAR        AS key_talent,
    d.value['EFFECTIVE_DATE']::DATE       AS effective_date,
    source._fivetran_synced::TIMESTAMP    AS uploaded_at,
    source._fivetran_deleted::BOOLEAN     AS is_deleted
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(KEY_TALENT_HISTORY_GROUP)) AS d

)

SELECT *
FROM renamed

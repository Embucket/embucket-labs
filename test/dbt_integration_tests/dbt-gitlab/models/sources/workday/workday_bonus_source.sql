WITH source AS (
  SELECT *
  FROM {{ source('workday','custom_bonus') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['BONUS_DATE']::DATE AS bonus_date,
    events.value['BONUS_TYPE']::VARCHAR AS bonus_type,
    events.value['DATE_TIME_INITIATED']::TIMESTAMP AS initiated_at,
    events.value['WORKDAY_ID']::VARCHAR AS workday_id
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.custom_bonus) AS events

)

SELECT *
FROM renamed

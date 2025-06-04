WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_bonus') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

      SELECT 
        d.value as data_by_row,
        uploaded_at
      FROM source,
      LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

      SELECT
           data_by_row['id']::NUMBER                 AS bonus_id,
           data_by_row['employeeId']::NUMBER         AS employee_id,
           data_by_row['customBonustype']::varchar   AS bonus_type,
           data_by_row['customBonusdate']::date      AS bonus_date,
           data_by_row['customNominatedBy']::varchar AS bonus_nominator_type,
           uploaded_at
      FROM intermediate
      WHERE bonus_date IS NOT NULL

)

SELECT *
FROM renamed

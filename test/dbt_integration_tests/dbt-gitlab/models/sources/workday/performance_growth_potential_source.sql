WITH source AS (

  SELECT *
  FROM {{ source('workday','performance_growth_potential') }}
  WHERE NOT _fivetran_deleted

), 

renamed AS (

  SELECT
    source.employee_id::NUMBER                      AS employee_id,
    d.value['GROWTH_POTENTIAL_RATING']::VARCHAR     AS growth_potential_rating,
    d.value['PERFORMANCE_RATING']::VARCHAR          AS performance_rating,
    d.value['WORKDAY_ID']::VARCHAR                  AS workday_id,
    d.value['Review_Period_-_Start_Date']::DATE     AS review_period_start_date,
    d.value['Review_Period_-_End_Date']::DATE       AS review_period_end_date
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(REVIEWS_ALL_STATUSES_GROUP)) AS d

)

SELECT * 
FROM renamed

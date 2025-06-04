WITH source AS (

  SELECT *
  FROM {{ source('pto', 'gitlab_pto') }}

),

deduped AS (

  SELECT *,
    IFF(MAX(REGEXP_LIKE(jsontext['team_member']['hris_id']::NUMBER,'[1-3][0-9]{4}')) OVER (PARTITION BY 'All') > 0, TRUE,FALSE)                      AS has_workday_ids,
    MIN(IFF(REGEXP_LIKE(jsontext['team_member']['hris_id']::NUMBER,'[1-3][0-9]{4}'),uploaded_at,NULL)) OVER(PARTITION BY 'All') AS workday_cutover_date
  FROM source
  QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['uuid']::VARCHAR ORDER BY uploaded_at DESC) = 1

),

each_pto_day AS (

  SELECT
    jsontext['end_date']::DATE                          AS end_date,
    jsontext['start_date']::DATE                        AS start_date,
    jsontext['status']::VARCHAR                         AS pto_status,
    jsontext['team_member']['day_length_hours']::NUMBER AS employee_day_length,
    jsontext['team_member']['hris_id']::NUMBER          AS hr_employee_id,
    jsontext['team_member']['uuid']::VARCHAR            AS employee_uuid,
    jsontext['uuid']::VARCHAR                           AS pto_uuid,
    ooo_days.value['date']::DATE                        AS pto_date,
    ooo_days.value['end_time']::TIMESTAMP               AS pto_ends_at,
    ooo_days.value['is_holiday']::BOOLEAN               AS is_holiday,
    ooo_days.value['recorded_hours']::NUMBER            AS recorded_hours,
    ooo_days.value['start_time']::TIMESTAMP             AS pto_starts_at,
    ooo_days.value['total_hours']::NUMBER               AS total_hours,
    jsontext['ooo_type']['group_type']::VARCHAR         AS pto_group_type,
    jsontext['ooo_type']['is_pto']::BOOLEAN             AS is_pto,
    jsontext['ooo_type']['name']::VARCHAR               AS pto_type_name,
    jsontext['ooo_type']['uuid']::VARCHAR               AS pto_type_uuid,
    jsontext['created_at']::TIMESTAMP                   AS pto_created_at
  FROM deduped,
    LATERAL FLATTEN(INPUT => jsontext['ooo_days']::ARRAY) AS ooo_days
  WHERE CASE
          WHEN has_workday_ids = TRUE
            AND year(end_date) >= '2024'
            AND uploaded_at < workday_cutover_date
          THEN FALSE
          ELSE TRUE
          END = TRUE           

)

SELECT *
FROM each_pto_day

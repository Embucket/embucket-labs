WITH source AS (

  SELECT 

    created_date::DATE AS created_date,
    version::VARCHAR AS version,
    period_id::VARCHAR AS period_id,
    period_name::VARCHAR AS period_name,
    assignment_id::VARCHAR AS assignment_id,
    title_name::VARCHAR AS title_name,
    employee_name::VARCHAR AS employee_name,
    quota_amount::NUMBER AS quota_amount,
    units::VARCHAR AS units,
    quota_type::VARCHAR AS quota_type,
    participant_id::VARCHAR AS participant_id,
    employee_id::VARCHAR AS employee_id,
    quota_amnt::NUMBER AS quota_amnt,
    hc_type::VARCHAR AS hc_type,
    region::VARCHAR AS region,
    role_type::VARCHAR AS role,
    start_date::DATE AS start_date,
    end_date:: DATE AS end_date

  FROM {{ source('sheetload','fy23_quota') }}

        )
SELECT * 
FROM source

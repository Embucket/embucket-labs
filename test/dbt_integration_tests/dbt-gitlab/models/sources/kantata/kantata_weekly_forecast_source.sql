WITH source AS (
  SELECT *
  FROM
    {{ source('kantata', 'forecast_ff_t_m_table_week') }}
),

renamed AS (
  SELECT
    "Account"::VARCHAR                                          AS account_name,
    "Opportunity"::VARCHAR                                      AS opportunity_id,
    "Opp Region"::VARCHAR                                       AS opportunity_region,
    "Lead"::VARCHAR                                             AS project_lead,
    "Project"::VARCHAR                                          AS project_name,
    "Project Components"::VARCHAR                               AS project_offering,
    "Free Services Approved"::VARCHAR                           AS project_investment,
    CASE
      WHEN project_investment = 'Investment Work' THEN 'Investment Services'
      ELSE 'Paid Services'
    END                                                         AS is_investment_service,
    "Project: ID"::NUMBER(38, 0)                                AS project_id,
    "9d.Billing Type"::VARCHAR                                  AS billing_type,
    "Week (Mon-Sun)"::VARCHAR                                   AS week_mon_sun,
    TO_DATE(SPLIT_PART(week_mon_sun, ' - ', 1), 'MON DD, YYYY') AS week_start_date,
    TO_DATE(SPLIT_PART(week_mon_sun, ' - ', 2), 'MON DD, YYYY') AS week_end_date,
    "FF Education"::NUMBER(38, 2)                               AS fixed_fee_education_forecast,
    "FF Consulting & PM"::NUMBER(38, 2)                         AS fixed_fee_consulting_forecast,
    "FF Other"::NUMBER(38, 2)                                   AS fixed_fee_other_forecast,
    "Allocations"::NUMBER(38, 2)                                AS allocations,
    "Forecast"::NUMBER(38, 2)                                   AS forecast,
    "Consulting Rollup"::NUMBER(38, 2)                          AS consulting_rollup,
    uploaded_at
  FROM
    source
  WHERE uploaded_at = (SELECT MAX(uploaded_at) FROM source)
)

SELECT *
FROM renamed

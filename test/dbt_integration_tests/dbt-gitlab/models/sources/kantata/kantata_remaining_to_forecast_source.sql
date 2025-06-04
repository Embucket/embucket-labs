WITH source AS (
  SELECT *
  FROM
    {{ source('kantata', 'remaining_to_forecast') }}
),

renamed AS (
  SELECT
    "Account"::VARCHAR                                 AS account_name,
    "Lead"::VARCHAR                                    AS project_lead,
    "Project"::VARCHAR                                 AS project_name,
    "Project Status"::VARCHAR                          AS project_status,
    "Opp Region"::VARCHAR                              AS region,
    "Segment"::VARCHAR                                 AS segment,
    "Billing Type"::VARCHAR                            AS billing_type,
    TO_DATE("SFDC Booking/ Closed Date", 'MM/DD/YYYY') AS sfdc_opportunity_close_date,
    "Project: ID"::NUMBER(38, 0)                       AS project_id,
    "Type"::NUMBER(38, 0)                              AS type,
    "Project Budget"::NUMBER(38, 2)                    AS project_budget,
    "FF Forecast"::NUMBER(38, 2)                       AS ff_forecast,
    "T&M Forecast"::NUMBER(38, 2)                      AS t_m_forecast,
    "Total Forecast"::NUMBER(38, 2)                    AS total_forecast,
    "Remaining to Forecast"::NUMBER(38, 2)             AS remaining_to_forecast,
    "Remaining Hours"::NUMBER(38, 0)                   AS remaining_hours,
    uploaded_at
  FROM
    source
  WHERE uploaded_at = (SELECT MAX(uploaded_at) FROM source)
)

SELECT *
FROM renamed

WITH source AS (
  SELECT *
  FROM
    {{ source('kantata', 'rev_qbr_details_by_project_user') }}
),

renamed AS (
  SELECT
    TO_DATE("Booking Date", 'MM/DD/YYYY')                       AS booking_date,
    "Region"::VARCHAR                                           AS region,
    "Project"::VARCHAR                                          AS project_name,
    "Project Components"::VARCHAR                               AS project_components,
    "Category"::VARCHAR                                         AS category,
    "Type"::VARCHAR                                             AS project_type,
    "Services"::VARCHAR                                         AS services,
    "User Type"::VARCHAR                                        AS user_type,
    "Role"::VARCHAR                                             AS user_role,
    "User Name"::VARCHAR                                        AS user_name,
    "Project: ID"::NUMBER(38, 0)                                AS project_id,
    "Free Services Approved"::VARCHAR                           AS project_investment,
    CASE
      WHEN project_investment = 'Investment Work' THEN 'Investment Services'
      ELSE 'Paid Services'
    END                                                         AS is_investment_service,
    "Project: Billing Mode Default"::VARCHAR                    AS billing_mode_default,
    "Week (Mon-Sun)/Year (Shared)"::VARCHAR                     AS week_mon_sun,
    TO_DATE(SPLIT_PART(week_mon_sun, ' - ', 1), 'MON DD, YYYY') AS week_start_date,
    TO_DATE(SPLIT_PART(week_mon_sun, ' - ', 2), 'MON DD, YYYY') AS week_end_date,
    "Hours"::NUMBER(38, 0)                                      AS hours_billed,
    "Fixed Fee"::NUMBER(38, 2)                                  AS fixed_fee_revenue,
    "T&M"::NUMBER(38, 2)                                        AS t_m_revenue,
    "Revenue"::NUMBER(38, 2)                                    AS revenue,
    uploaded_at
  FROM
    source
  WHERE uploaded_at = (SELECT MAX(uploaded_at) FROM source)
)

SELECT *
FROM renamed

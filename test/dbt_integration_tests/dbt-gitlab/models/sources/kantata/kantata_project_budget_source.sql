WITH source AS (
  SELECT *
  FROM
    {{ source('kantata', 'nc_project_budget_details') }}
),

renamed AS (
  SELECT
    "Project"::VARCHAR                                 AS project_name,
    "Project: ID"::NUMBER(38, 0)                       AS id,
    "GitLab User Type"::VARCHAR                        AS user_type,
    "Project: Budget Used"::NUMBER(38, 2)              AS budget_used,
    "Project: Over Budget"::BOOLEAN                    AS is_over_budget,
    "Project: Current Status Message"::VARCHAR         AS current_status_message,
    "Project: Archived"::BOOLEAN                       AS is_archived,
    "Project: Rate Card Name"::VARCHAR                 AS rate_card,
    "Project: Billing Mode Default"::VARCHAR           AS billing_mode_default,
    "Internal or Partner"::VARCHAR                     AS delivery_method,
    "2. Opportunity"::VARCHAR                          AS opportunity_id,
    "Free Services Approved"::VARCHAR                  AS project_investment,
    CASE
      WHEN project_investment = 'Investment Work' THEN 'Investment Services'
      ELSE 'Paid Services'
    END                                                AS is_investment_service,
    TO_DATE("Date (Project Start)", 'MM/DD/YYYY')      AS project_start,
    TO_DATE("Date (Project Due)", 'MM/DD/YYYY')        AS project_due,
    TO_DATE("Date (Project Completed)", 'MM/DD/YYYY')  AS project_completed,
    TO_DATE("Date (Project Created)", 'MM/DD/YYYY')    AS project_created,
    "9C. Project Components"::VARCHAR                  AS project_components,
    "Bill Rate: Time Entries, Billable"::NUMBER(38, 2) AS bill_rate,
    uploaded_at
  FROM
    source
  WHERE uploaded_at = (SELECT MAX(uploaded_at) FROM source)
)

SELECT *
FROM renamed

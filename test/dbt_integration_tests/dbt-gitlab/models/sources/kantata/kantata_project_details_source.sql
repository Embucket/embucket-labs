WITH source AS (
  SELECT *
  FROM
    {{ source('kantata', 'nc_project_details_table_including_custom_fields') }}
),

renamed AS (
  SELECT
    "Project"::VARCHAR                                  AS project_name,
    "Project: ID"::NUMBER(38, 0)                        AS id,
    "Project: Client Name"::VARCHAR                     AS client_name,
    "GitLab User Type"::VARCHAR                         AS user_type,
    "1. Account"::VARCHAR                               AS account_name,
    "2. Opportunity"::VARCHAR                           AS opportunity_id,
    "7. Engagement Manager"::VARCHAR                    AS engagement_manager_name,
    "8. GitLab Project Link"::VARCHAR                   AS gitlab_project_link,
    "8. SFDC PS Description"::VARCHAR                   AS sfdc_ps_description,
    "9b. Invoice Number"::VARCHAR                       AS invoice_number,
    "9c. Processed Change Order"::VARCHAR               AS processed_change_order,
    "9S. Status Color"::VARCHAR                         AS status_color,
    "9S. Sponsor Notes"::VARCHAR                        AS sponsor_notes,
    "9X. Project Retrospective"::VARCHAR                AS project_retrospective,
    "9X1. If 'No' Project Retrospective, Why?"::VARCHAR AS if_no_project_retrospective_why,
    "Free Services Approved"::VARCHAR                   AS project_investment,
    CASE
      WHEN project_investment = 'Investment Work' THEN 'Investment Services'
      ELSE 'Paid Services'
    END                                                 AS is_investment_service,
    "8. SFDC Project Scope"::VARCHAR                    AS sfdc_project_scope,
    "8. SFDC Scoping Issue Link"::VARCHAR               AS sfdc_scoping_issue_link,
    "9h. Security Requirement"::VARCHAR                 AS security_requirement,
    "9M. Project Staffing"::VARCHAR                     AS project_staffing,
    uploaded_at
  FROM
    source
  WHERE uploaded_at = (SELECT MAX(uploaded_at) FROM source)
)

SELECT *
FROM renamed

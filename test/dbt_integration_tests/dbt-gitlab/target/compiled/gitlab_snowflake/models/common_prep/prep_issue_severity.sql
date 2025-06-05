


WITH gitlab_dotcom_issue_severity_source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_issuable_severities_source

), renamed AS (

    SELECT
      gitlab_dotcom_issue_severity_source.issue_severity_id     AS dim_issue_severity_id,
      gitlab_dotcom_issue_severity_source.issue_id              AS issue_id,
      gitlab_dotcom_issue_severity_source.severity              AS severity
    FROM gitlab_dotcom_issue_severity_source

)

SELECT
      *,
      '@dtownsend'::VARCHAR       AS created_by,
      '@michellecooper'::VARCHAR       AS updated_by,
      '2021-08-04'::DATE        AS model_created_date,
      '2023-09-29'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM renamed
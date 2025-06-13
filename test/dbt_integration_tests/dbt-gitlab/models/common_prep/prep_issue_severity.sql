{{ config(
    tags=["product"]
) }}


WITH gitlab_dotcom_issue_severity_source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issuable_severities_source')}}

), renamed AS (

    SELECT
      gitlab_dotcom_issue_severity_source.issue_severity_id     AS dim_issue_severity_id,
      gitlab_dotcom_issue_severity_source.issue_id              AS issue_id,
      gitlab_dotcom_issue_severity_source.severity              AS severity
    FROM gitlab_dotcom_issue_severity_source

)

{{ dbt_audit(
    cte_ref="renamed",
    created_by="@dtownsend",
    updated_by="@michellecooper",
    created_date="2021-08-04",
    updated_date="2023-09-29"
) }}

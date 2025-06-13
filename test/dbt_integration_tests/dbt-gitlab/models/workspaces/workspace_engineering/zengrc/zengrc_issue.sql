{{ simple_cte([
    ('zengrc_issue_source', 'zengrc_issue_source'),
    ('prep_issue', 'prep_issue')
]) }}

, joined AS (

  SELECT
    zengrc_issue_source.gitlab_dotcom_issue_id,
    prep_issue.dim_issue_sk,
    zengrc_issue_source.issue_id,
    zengrc_issue_source.issue_code,
    zengrc_issue_source.issue_created_at,
    zengrc_issue_source.issue_description,
    zengrc_issue_source.issue_notes,
    zengrc_issue_source.issue_status,
    zengrc_issue_source.issue_stop_date,
    zengrc_issue_source.issue_tags,
    zengrc_issue_source.issue_title,
    zengrc_issue_source.zengrc_object_type,
    zengrc_issue_source.issue_updated_at,
    zengrc_issue_source.issue_loaded_at,
    zengrc_issue_source.remediation_recommendations,
    zengrc_issue_source.deficiency_range,
    zengrc_issue_source.risk_rating,
    zengrc_issue_source.observation_issue_owner,
    zengrc_issue_source.likelihood,
    zengrc_issue_source.impact,
    zengrc_issue_source.gitlab_issue_url,
    zengrc_issue_source.gitlab_assignee,
    zengrc_issue_source.department,
    zengrc_issue_source.type_of_deficiency,
    zengrc_issue_source.internal_control_component,
    zengrc_issue_source.severity_of_deficiency,
    zengrc_issue_source.financial_system_line_item,
    zengrc_issue_source.is_reported_to_audit_committee,
    zengrc_issue_source.deficiency_theme,
    zengrc_issue_source.remediated_evidence,
    zengrc_issue_source.financial_assertion_affected_by_deficiency,
    zengrc_issue_source.compensating_controls
  FROM zengrc_issue_source
  LEFT JOIN prep_issue
    ON zengrc_issue_source.gitlab_dotcom_issue_id = prep_issue.issue_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@pempey",
    updated_by="@michellecooper",
    created_date="2022-01-20",
    updated_date="2023-09-29"
) }}
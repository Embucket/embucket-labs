{{ config(
    tags=["product"]
) }}

{{ simple_cte([
    ('prep_issue', 'prep_issue'),
    ('prep_epic', 'prep_epic'),
    ('gitlab_dotcom_epic_issues_source', 'gitlab_dotcom_epic_issues_source')
]) }}

, final AS (

    SELECT
      -- Surrogate Key
      {{ dbt_utils.generate_surrogate_key(['gitlab_dotcom_epic_issues_source.epic_issues_relation_id']) }}  AS dim_epic_issue_sk,

      -- Natural Key
      gitlab_dotcom_epic_issues_source.epic_issues_relation_id                                     AS epic_issue_id,

      -- Foreign Keys
      prep_issue.dim_issue_sk,
      prep_epic.dim_epic_sk,
      gitlab_dotcom_epic_issues_source.issue_id,
      gitlab_dotcom_epic_issues_source.epic_id,

      -- Other Attributes
      gitlab_dotcom_epic_issues_source.epic_issue_relative_position                                AS epic_issue_relative_position

    FROM gitlab_dotcom_epic_issues_source
    LEFT JOIN prep_issue
      ON gitlab_dotcom_epic_issues_source.issue_id = prep_issue.issue_id
    LEFT JOIN prep_epic
      ON gitlab_dotcom_epic_issues_source.epic_id = prep_epic.epic_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2023-09-07",
    updated_date="2023-09-29"
) }}

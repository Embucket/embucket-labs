{{ config(
    tags=["mnpi_exception"]
) }}

WITH prep_namespace AS (

    SELECT *
    FROM {{ ref('prep_namespace') }}
    WHERE is_currently_valid = TRUE
  
), issue_links AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issue_links_source') }}
    WHERE is_currently_valid

), issue_notes AS (
  
    SELECT
      noteable_id AS issue_id,
      *
    FROM {{ ref('gitlab_dotcom_notes_source') }}
    WHERE noteable_type = 'Issue'
      AND system = FALSE
  
), gitlab_epics AS (

    SELECT *
    FROM {{ ref('prep_epic') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_namespace_sk, epic_internal_id ORDER BY created_at DESC) = 1

), gitlab_issues AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_issues_source') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY project_id, issue_iid ORDER BY created_at DESC) = 1 

), collaboration_projects AS (

    SELECT
      account_id,
      gitlab_customer_success_project
    FROM {{ ref('sfdc_account_source') }}
    WHERE gitlab_customer_success_project LIKE '%gitlab.com/%'

), gitlab_dotcom_project_routes AS (

    SELECT
      'https://gitlab.com/' || path AS complete_path,
      source_id                     AS project_id,
      *
    FROM {{ ref('gitlab_dotcom_routes_source') }}
    WHERE source_type = 'Project'

), gitlab_dotcom_namespace_routes AS (

    SELECT
      'https://gitlab.com/' || path AS complete_path,
      source_id                     AS namespace_id,
      gitlab_dotcom_routes_source.*,
      prep_namespace.dim_namespace_sk
    FROM {{ ref('gitlab_dotcom_routes_source') }}
    LEFT JOIN prep_namespace
      ON gitlab_dotcom_routes_source.source_id = prep_namespace.namespace_id
    WHERE source_type = 'Namespace' 

), collaboration_projects_with_ids AS (

    SELECT
      collaboration_projects.*,
      gitlab_dotcom_project_routes.project_id                              AS collaboration_project_id,
      gitlab_issues.issue_id,
      gitlab_issues.issue_description,
      IFNULL(gitlab_issues.issue_last_edited_at, gitlab_issues.created_at) AS updated_at
    FROM collaboration_projects
    LEFT JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.complete_path = collaboration_projects.gitlab_customer_success_project
    LEFT JOIN gitlab_issues
      ON gitlab_issues.project_id = gitlab_dotcom_project_routes.project_id

), collaboration_projects_issue_descriptions AS (

    SELECT
      *,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_description, '(?<=gitlab.com\/groups\/)gitlab-org\/[^ ]*epics\/[0-9]{1,10}') AS epic_links
    FROM collaboration_projects_with_ids
    WHERE ARRAY_SIZE(epic_links) != 0

), collaboration_projects_issue_descriptions_parsed AS (

    SELECT
      collaboration_projects_issue_descriptions.*,
      f.value                                                                      AS user_request_issue_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss') AS user_request_epic_path_fixed,
      SPLIT_PART(f.value, '/', -1)::NUMBER                                         AS user_request_epic_internal_id,
      RTRIM(SPLIT_PART(f.value, '/epics', 1), '/-')                                AS user_request_namespace_path
    FROM collaboration_projects_issue_descriptions,
      TABLE(FLATTEN(epic_links)) f

), collaboration_projects_issue_notes AS (

    SELECT 
      collaboration_projects_with_ids.*,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_notes.note, '(?<=gitlab.com\/groups\/)gitlab-org\/[^ ]*epics\/[0-9]{1,10}') AS epic_links,
      issue_notes.updated_at                                                       AS note_updated_at
    FROM collaboration_projects_with_ids
    LEFT JOIN issue_notes
      ON issue_notes.issue_id = collaboration_projects_with_ids.issue_id
    WHERE ARRAY_SIZE(epic_links) != 0

), collaboration_projects_issue_notes_parsed AS (

    SELECT
      collaboration_projects_issue_notes.*,
      f.value                                                                      AS user_request_epic_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss') AS user_request_epic_path_fixed,
      SPLIT_PART(f.value, '/', -1)::NUMBER                                         AS user_request_epic_internal_id,
      RTRIM(SPLIT_PART(f.value, '/epics', 1), '/-')                                AS user_request_namespace_path
    FROM collaboration_projects_issue_notes,
      TABLE(FLATTEN(epic_links)) f

), collaboration_projects_issue_description_notes_unioned AS (

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_epic_internal_id,
      user_request_namespace_path,
      note_updated_at AS link_last_updated_at
    FROM collaboration_projects_issue_notes_parsed

    UNION

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_epic_internal_id,
      user_request_namespace_path,
      updated_at
    FROM collaboration_projects_issue_descriptions_parsed

), unioned_with_user_request_namespace_id AS (

    SELECT
      collaboration_projects_issue_description_notes_unioned.*,
      gitlab_dotcom_namespace_routes.namespace_id AS user_request_namespace_id,
      gitlab_dotcom_namespace_routes.dim_namespace_sk AS user_request_namespace_sk
    FROM collaboration_projects_issue_description_notes_unioned
    INNER JOIN gitlab_dotcom_namespace_routes
      ON gitlab_dotcom_namespace_routes.path = collaboration_projects_issue_description_notes_unioned.user_request_namespace_path
    INNER JOIN prep_namespace
      ON prep_namespace.namespace_id = gitlab_dotcom_namespace_routes.namespace_id

), final AS ( -- In case there are various issues with the same link to issues and dim_crm_account_id, dedup them by taking the latest updated link

    SELECT
      gitlab_epics.epic_id                                                    AS epic_id,
      gitlab_epics.dim_epic_sk                                                AS dim_epic_sk,
      unioned_with_user_request_namespace_id.account_id                       AS dim_crm_account_id,
      unioned_with_user_request_namespace_id.collaboration_project_id         AS dim_collaboration_project_id,
      unioned_with_user_request_namespace_id.user_request_namespace_id        AS dim_namespace_id,
      unioned_with_user_request_namespace_id.gitlab_customer_success_project  AS gitlab_customer_success_project,
      unioned_with_user_request_namespace_id.user_request_epic_internal_id    AS epic_internal_id,
      unioned_with_user_request_namespace_id.link_last_updated_at             AS link_last_updated_at
    FROM unioned_with_user_request_namespace_id
    INNER JOIN gitlab_epics
      ON gitlab_epics.dim_namespace_sk = unioned_with_user_request_namespace_id.user_request_namespace_sk
      AND gitlab_epics.epic_internal_id = unioned_with_user_request_namespace_id.user_request_epic_internal_id
    QUALIFY ROW_NUMBER() OVER(PARTITION BY gitlab_epics.epic_id, unioned_with_user_request_namespace_id.account_id
      ORDER BY unioned_with_user_request_namespace_id.link_last_updated_at DESC NULLS LAST) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-10-12",
    updated_date="2023-09-08",
) }}


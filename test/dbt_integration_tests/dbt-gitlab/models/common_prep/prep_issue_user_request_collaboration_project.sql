{{ config(
    tags=["mnpi_exception"]
) }}

WITH gitlab_dotcom_projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects_source') }}
  
), map_moved_duplicated_issue AS (

    SELECT *
    FROM {{ ref('map_moved_duplicated_issue') }}

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

), prep_project AS (

    SELECT *
    FROM {{ ref('prep_project') }}

), gitlab_issues AS (

    SELECT
      prep_issue.*
    FROM {{ ref('prep_issue') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY prep_issue.dim_project_sk, prep_issue.issue_internal_id ORDER BY prep_issue.created_at DESC) = 1

), collaboration_projects AS (

    SELECT
      account_id,
      gitlab_customer_success_project
    FROM {{ ref('sfdc_account_source') }}
    WHERE gitlab_customer_success_project LIKE '%gitlab.com/%'

), gitlab_dotcom_project_routes AS (

    SELECT
      'https://gitlab.com/' || gitlab_dotcom_routes_source.path AS complete_path,
      gitlab_dotcom_routes_source.source_id                     AS project_id,
      prep_project.dim_project_sk,
      gitlab_dotcom_routes_source.*
    FROM {{ ref('gitlab_dotcom_routes_source') }}
    LEFT JOIN prep_project
      ON gitlab_dotcom_routes_source.source_id = prep_project.project_id
    WHERE source_type = 'Project'

), collaboration_projects_with_ids AS (

    SELECT
      collaboration_projects.*,
      gitlab_dotcom_project_routes.project_id                              AS collaboration_project_id,
      gitlab_issues.issue_id,
      gitlab_issues.dim_issue_sk,
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
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_description, '(?<=gitlab.com\/)gitlab-org\/[^ ]*issues\/[0-9]{1,10}')      AS issue_links
    FROM collaboration_projects_with_ids
    WHERE ARRAY_SIZE(issue_links) != 0

), collaboration_projects_issue_descriptions_parsed AS (

    SELECT
      collaboration_projects_issue_descriptions.*,
      f.value AS user_request_issue_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss') AS user_request_issue_path_fixed,
      SPLIT_PART(f.value, '/', -1)::NUMBER                                         AS user_request_issue_iid,
      RTRIM(SPLIT_PART(f.value, '/issues', 1), '/-')                               AS user_request_project_path
    FROM collaboration_projects_issue_descriptions,
      TABLE(FLATTEN(issue_links)) f

), collaboration_projects_issue_notes AS (

    SELECT 
      collaboration_projects_with_ids.*,
      "{{this.database}}".{{target.schema}}.regexp_to_array(issue_notes.note, '(?<=gitlab.com\/)gitlab-org\/[^ ]*issues\/[0-9]{1,10}') AS issue_links,
      issue_notes.updated_at                                                        AS note_updated_at
    FROM collaboration_projects_with_ids
    LEFT JOIN issue_notes
      ON issue_notes.issue_id = collaboration_projects_with_ids.issue_id
    WHERE ARRAY_SIZE(issue_links) != 0

), collaboration_projects_issue_notes_parsed AS (

    SELECT
      collaboration_projects_issue_notes.*,
      f.value AS user_request_issue_path,
      REPLACE(REPLACE(f.value, 'gitlab-ee', 'gitlab'), 'gitlab-ce', 'gitlab-foss')  AS user_request_issue_path_fixed,
      SPLIT_PART(f.value, '/', -1)::NUMBER                                          AS user_request_issue_iid,
      RTRIM(SPLIT_PART(f.value, '/issues', 1), '/-')                                AS user_request_project_path
    FROM collaboration_projects_issue_notes,
      TABLE(FLATTEN(issue_links)) f

), collaboration_projects_issue_description_notes_unioned AS (

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_issue_iid,
      user_request_project_path,
      note_updated_at  AS link_last_updated_at
    FROM collaboration_projects_issue_notes_parsed

    UNION

    SELECT
      account_id,
      gitlab_customer_success_project,
      collaboration_project_id,
      user_request_issue_iid,
      user_request_project_path,
      updated_at
    FROM collaboration_projects_issue_descriptions_parsed

), unioned_with_user_request_project_id AS (

    SELECT
      collaboration_projects_issue_description_notes_unioned.*,
      gitlab_dotcom_project_routes.project_id AS user_request_project_id,
      gitlab_dotcom_project_routes.dim_project_sk AS user_request_project_sk
    FROM collaboration_projects_issue_description_notes_unioned
    INNER JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.path = collaboration_projects_issue_description_notes_unioned.user_request_project_path

), unioned_with_issue_links AS (

    SELECT
      gitlab_issues.issue_id                                                AS issue_id,
      gitlab_issues.dim_issue_sk,
      unioned_with_user_request_project_id.account_id                       AS dim_crm_account_id,
      unioned_with_user_request_project_id.collaboration_project_id         AS dim_collaboration_project_id,
      unioned_with_user_request_project_id.user_request_project_id          AS dim_project_id,
      unioned_with_user_request_project_id.gitlab_customer_success_project,
      unioned_with_user_request_project_id.user_request_issue_iid           AS issue_internal_id,
      unioned_with_user_request_project_id.link_last_updated_at
    FROM unioned_with_user_request_project_id
    INNER JOIN gitlab_issues
      ON gitlab_issues.dim_project_sk = unioned_with_user_request_project_id.user_request_project_sk
      AND gitlab_issues.issue_internal_id = unioned_with_user_request_project_id.user_request_issue_iid

    UNION

    SELECT
      gitlab_issues.issue_id                                                AS issue_id,
      gitlab_issues.dim_issue_sk,
      collaboration_projects_with_ids.account_id                            AS dim_crm_account_id,
      collaboration_projects_with_ids.collaboration_project_id              AS dim_collaboration_project_id,
      gitlab_issues.project_id                                              AS dim_project_id,
      collaboration_projects_with_ids.gitlab_customer_success_project,
      gitlab_issues.issue_internal_id                                       AS issue_internal_id,
      issue_links.updated_at                                                AS link_last_updated_at
    FROM collaboration_projects_with_ids
    INNER JOIN issue_links
      ON issue_links.source_id = collaboration_projects_with_ids.issue_id
    INNER JOIN gitlab_issues
      ON gitlab_issues.issue_id = issue_links.target_id
    INNER JOIN gitlab_dotcom_project_routes
      ON gitlab_dotcom_project_routes.project_id = gitlab_issues.project_id
    WHERE gitlab_dotcom_project_routes.path LIKE 'gitlab-org%'

), final AS ( -- In case there are various issues that merge to the same, dedup them by taking the latest updated link

    SELECT
      map_moved_duplicated_issue.issue_id,
      map_moved_duplicated_issue.dim_issue_sk,
      unioned_with_issue_links.dim_crm_account_id,
      unioned_with_issue_links.dim_collaboration_project_id,
      unioned_with_issue_links.dim_project_id                 AS dim_original_issue_project_id,
      unioned_with_issue_links.gitlab_customer_success_project,
      unioned_with_issue_links.issue_internal_id              AS original_issue_internal_id,
      unioned_with_issue_links.link_last_updated_at           AS link_last_updated_at
    FROM unioned_with_issue_links
    INNER JOIN map_moved_duplicated_issue
      ON map_moved_duplicated_issue.dim_issue_sk = unioned_with_issue_links.dim_issue_sk
    QUALIFY ROW_NUMBER() OVER(PARTITION BY map_moved_duplicated_issue.dim_issue_sk, unioned_with_issue_links.dim_crm_account_id
      ORDER BY unioned_with_issue_links.link_last_updated_at DESC NULLS LAST) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jpeguero",
    updated_by="@michellecooper",
    created_date="2021-10-12",
    updated_date="2023-09-29"
) }}


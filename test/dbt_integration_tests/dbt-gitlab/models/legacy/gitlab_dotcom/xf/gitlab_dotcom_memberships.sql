

WITH members AS ( -- direct group and project members

    SELECT *
    FROM {{ ref('gitlab_dotcom_members') }} members
    WHERE is_currently_valid = TRUE
      AND user_id IS NOT NULL
      AND {{ filter_out_blocked_users('members', 'user_id') }}
    QUALIFY RANK() OVER (
        PARTITION BY 
          user_id, 
          source_id, 
          member_source_type 
        ORDER BY 
          access_level DESC, 
          invite_created_at DESC
        ) = 1
    
), namespaces AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespaces') }}

), namespace_lineage AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_namespace_lineage') }}

), users AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_users') }}

), projects AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_projects') }}

), group_group_links AS ( -- groups invited to groups

    SELECT *
    FROM {{ ref('gitlab_dotcom_group_group_links') }}
    WHERE is_currently_valid = TRUE

), project_group_links AS ( -- groups invited to projects

    SELECT *
    FROM {{ ref('gitlab_dotcom_project_group_links') }}
    WHERE is_currently_valid = TRUE

), group_group_links_lineage AS (

    SELECT
      group_group_links.shared_group_id, -- the "host" group
      group_group_links.group_group_link_id,
      group_group_links.shared_with_group_id, -- the "guest" group
      group_group_links.group_access,
      namespace_lineage.upstream_lineage AS base_and_ancestors -- all parent namespaces for the "guest" group
    FROM group_group_links
    INNER JOIN namespace_lineage
      ON group_group_links.shared_with_group_id = namespace_lineage.namespace_id

), project_group_links_lineage AS (

    SELECT
      projects.namespace_id              AS shared_group_id, -- the "host" group the project directly belongs to
      project_group_links.project_group_link_id,
      project_group_links.group_id       AS shared_with_group_id, -- the "guest" group
      project_group_links.group_access,
      namespace_lineage.upstream_lineage AS base_and_ancestors -- all parent namespaces for the "guest" group
    FROM project_group_links
    INNER JOIN projects
      ON project_group_links.project_id = projects.project_id
    INNER JOIN namespace_lineage
      ON project_group_links.group_id = namespace_lineage.namespace_id

), group_group_links_flattened AS (

    SELECT
      group_group_links_lineage.*,
      f.value AS shared_with_group_lineage -- creates one row for each "guest" group and its parent namespaces
    FROM group_group_links_lineage,
      TABLE(FLATTEN(group_group_links_lineage.base_and_ancestors)) f

), project_group_links_flattened AS (

    SELECT
      project_group_links_lineage.*,
      f.value AS shared_with_group_lineage -- creates one row for each "guest" group and its parent namespaces
    FROM project_group_links_lineage,
      TABLE(FLATTEN(project_group_links_lineage.base_and_ancestors)) f

), group_members AS (

    SELECT *
    FROM members
    WHERE member_source_type = 'Namespace'

), project_members AS (

    SELECT
      projects.namespace_id,
      members.*
    FROM members
    INNER JOIN projects
      ON members.source_id = projects.project_id
    WHERE member_source_type = 'Project'

), group_group_link_members AS (

    SELECT *
    FROM group_group_links_flattened
    INNER JOIN group_members
      ON group_group_links_flattened.shared_with_group_lineage = group_members.source_id

), project_group_link_members AS (

    SELECT *
    FROM project_group_links_flattened
    INNER JOIN group_members
      ON project_group_links_flattened.shared_with_group_lineage = group_members.source_id

), individual_namespaces AS (

    SELECT *
    FROM namespaces
    WHERE namespace_type = 'User'

), unioned AS (

    SELECT
      source_id          AS namespace_id,
      'group_membership' AS membership_source_type,
      source_id          AS membership_source_id,
      access_level,
      NULL               AS group_access, -- direct member of group
      requested_at,
      user_id
    FROM group_members
  
    UNION
  
    SELECT
      namespace_id,
      'project_membership' AS membership_source_type,
      source_id            AS membership_source_id,
      access_level,
      NULL                 AS group_access, -- direct member of project
      requested_at,
      user_id
    FROM project_members
  
    UNION
  
    SELECT
      shared_group_id     AS namespace_id,
      IFF(
          shared_with_group_lineage = shared_with_group_id, 
          'group_group_link', 
          'group_group_link_ancestor'
      )                   AS membership_source_type, -- differentiate "guest" group from its parent namespaces
      group_group_link_id AS membership_source_id,
      access_level,
      group_access,
      requested_at,
      user_id
    FROM group_group_link_members
  
    UNION
  
    SELECT
      shared_group_id       AS namespace_id,
      IFF(
          shared_with_group_lineage = shared_with_group_id, 
          'project_group_link', 
          'project_group_link_ancestor'
      )                     AS membership_source_type, -- differentiate "guest" group from its parent namespaces
      project_group_link_id AS membership_source_id,
      access_level,
      group_access,
      requested_at,
      user_id
    FROM project_group_link_members
    
    UNION
  
    SELECT
      namespace_id,
      'individual_namespace' AS membership_source_type,
      namespace_id           AS membership_source_id,
      50                     AS access_level, -- implied by ownership
      NULL                   AS group_access, -- implied by ownership
      NULL                   AS requested_at, -- implied by ownership
      owner_id               AS user_id
    FROM individual_namespaces
  
), joined AS (

    SELECT
      namespace_lineage.ultimate_parent_id,
      namespace_lineage.ultimate_parent_plan_id,
      namespace_lineage.ultimate_parent_plan_title,
      unioned.*,
      users.state AS user_state,
      users.user_type
    FROM unioned
    INNER JOIN namespace_lineage
      ON unioned.namespace_id = namespace_lineage.namespace_id
    INNER JOIN users
      ON unioned.user_id = users.user_id

), final AS (

    SELECT
      ultimate_parent_id,
      ultimate_parent_plan_id,
      ultimate_parent_plan_title,
      namespace_id,
      membership_source_type,
      membership_source_id,
      access_level,
      group_access,
      requested_at,
      user_id,
      user_state,
      user_type,
      IFF(access_level = 10 OR group_access = 10, TRUE, FALSE) AS is_guest, -- exclude any user with guest access
      IFF(
          user_state = 'active' AND (user_type != 'Project Bot' OR user_type IS NULL) AND requested_at IS NULL, 
          TRUE, FALSE -- must be active, not a project bot, and not awaiting access
      )                                                        AS is_active,
      IFF(
          (ultimate_parent_plan_title = 'gold' AND is_active = TRUE AND is_guest = FALSE)
          OR (ultimate_parent_plan_title != 'gold' AND is_active = TRUE),
          TRUE, FALSE -- exclude guests if namespace has gold plan
      )                                                        AS is_billable
    FROM joined  
      
)

SELECT *
FROM final

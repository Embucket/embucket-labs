

WITH notes AS (

  SELECT
    {{ dbt_utils.star(from=ref('gitlab_dotcom_notes') ) }},
    created_at AS note_created_at,
    updated_at AS note_updated_at,
    "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=(gitlab.my.|na34.)salesforce.com\/)[0-9a-zA-Z]{15,18}') AS sfdc_link_array,
    "{{this.database}}".{{target.schema}}.regexp_to_array(note, '(?<=gitlab.zendesk.com\/agent\/tickets\/)[0-9]{1,18}') AS zendesk_link_array
  FROM {{ ref('gitlab_dotcom_notes') }}
  WHERE noteable_type IN ('Epic', 'Issue', 'MergeRequest')

)

, projects AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_projects_xf') }}

)

, epics AS (

  SELECT *
  FROM {{ ref('prep_epic') }}

)

, internal_namespaces AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_namespaces_xf') }}
  WHERE namespace_is_internal

)

, notes_with_namespaces AS (

  SELECT
    notes.*,
    projects.project_name,
    internal_namespaces.namespace_id,
    internal_namespaces.namespace_name

  FROM notes
  LEFT JOIN projects
    ON notes.noteable_type IN ('Issue', 'MergeRequest')
    AND notes.project_id = projects.project_id
  LEFT JOIN epics
    ON notes.noteable_type = 'Epic'
    AND notes.noteable_id = epics.epic_id
  INNER JOIN internal_namespaces
    ON COALESCE(projects.ultimate_parent_id, epics.ultimate_parent_namespace_id) = internal_namespaces.namespace_id

)

SELECT *
FROM notes_with_namespaces

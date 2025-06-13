WITH notes AS (

  SELECT
    {{ dbt_utils.star(from=ref('gitlab_dotcom_notes_source') ) }},
    created_at AS note_created_at,
    updated_at AS note_updated_at
  FROM {{ ref('gitlab_dotcom_notes_source') }}
  WHERE noteable_type IN ('Epic', 'Issue', 'MergeRequest')

),

projects AS (

  SELECT *
  FROM {{ ref('map_project_internal') }}

),

epics AS (

  SELECT *
  FROM {{ ref('map_epic_internal') }}

),


notes_with_namespaces AS (

  SELECT
    notes.*,
    COALESCE(epics.ultimate_parent_namespace_id, projects.ultimate_parent_namespace_id) AS ultimate_parent_namespace_id,
    COALESCE(epics.namespace_id, projects.parent_namespace_id)                          AS namespace_id
  FROM notes
  LEFT JOIN projects
    ON notes.noteable_type IN ('Issue', 'MergeRequest')
      AND notes.project_id = projects.project_id
  LEFT JOIN epics
    ON notes.noteable_type = 'Epic'
      AND notes.noteable_id = epics.epic_id
  WHERE COALESCE(epics.ultimate_parent_namespace_id, projects.ultimate_parent_namespace_id) IS NOT NULL

)

SELECT *
FROM notes_with_namespaces

WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_gitlab_roulette_capacity_history_source') }}

),

report AS (

  SELECT
    unique_id,
    gitlab_roulette_project,
    gitlab_roulette_role,
    gitlab_roulette_history_at,
    total_team_members,
    available_team_members,
    hungry_team_members,
    reduced_capacity_team_members,
    ooo_team_members,
    no_capacity_team_members,
    updated_at,
    valid_from,
    COALESCE(valid_to, {{ var('tomorrow') }}) AS valid_to
  FROM source
  WHERE is_current = TRUE

)

SELECT *
FROM report

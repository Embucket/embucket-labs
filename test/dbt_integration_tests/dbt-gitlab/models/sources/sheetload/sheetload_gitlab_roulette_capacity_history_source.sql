WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_gitlab_roulette_capacity_history_snapshot') }}

),

renamed AS (

  SELECT
    unique_id::VARCHAR AS unique_id,
    project::VARCHAR AS gitlab_roulette_project,
    role::VARCHAR AS gitlab_roulette_role,
    timestamp::TIMESTAMP AS gitlab_roulette_history_at,
    total_team_members::NUMBER AS total_team_members,
    available_team_members::NUMBER AS available_team_members,
    hungry_team_members::NUMBER AS hungry_team_members,
    reduced_capacity_team_members::NUMBER AS reduced_capacity_team_members,
    ooo_team_members::NUMBER AS ooo_team_members,
    no_capacity_team_members::NUMBER AS no_capacity_team_members,
    updated_at::TIMESTAMP AS updated_at,
    dbt_valid_from::TIMESTAMP AS valid_from,
    dbt_valid_to::TIMESTAMP AS valid_to,
    IFF(dbt_valid_to IS NULL, TRUE, FALSE) AS is_current
  FROM source

)

SELECT *
FROM renamed

WITH source AS (

  SELECT *
  FROM {{ source('snapshots','workday_supervisory_org_snapshots') }}

), renamed AS (

  SELECT
    team_id::VARCHAR                  AS team_id,
    team_hierarchy_level::NUMBER      AS team_hierarchy_level,
    team_members_count::NUMBER        AS team_members_count,
    team_manager_inherited::BOOLEAN   AS is_team_manager_inherited,
    team_inactivated::BOOLEAN          AS is_team_inactivated,
    team_manager_name::VARCHAR        AS team_manager_name,
    team_name::VARCHAR                AS team_name,
    team_manager_name_id::NUMBER      AS team_manager_name_id,
    team_superior_team_id::VARCHAR    AS team_superior_team_id,
    team_inactivated_date::TIMESTAMP  AS team_inactivated_date,
    _fivetran_deleted::BOOLEAN        AS is_deleted,
    _fivetran_synced::TIMESTAMP       AS uploaded_at,
    dbt_valid_from::TIMESTAMP         AS valid_from,
    dbt_valid_to::TIMESTAMP           AS valid_to
  FROM source

)

SELECT *
FROM renamed

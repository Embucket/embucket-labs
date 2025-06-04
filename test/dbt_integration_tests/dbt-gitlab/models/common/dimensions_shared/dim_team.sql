WITH source AS (

  SELECT *
  FROM {{ ref('workday_supervisory_organizations_source') }}

),

team_data AS (

  SELECT
    source.team_id,
    source.team_superior_team_id,
    source.team_hierarchy_level,
    source.is_team_inactivated,
    source.team_name,
    source.team_manager_name,
    source.team_manager_name_id,
    source.team_members_count,
    source.is_team_manager_inherited,
    IFF(
      source.is_deleted AND source.team_inactivated_date IS NULL,
      source.uploaded_at,
      source.team_inactivated_date)                                       AS team_inactivated_date,
    source.is_deleted,
    DATE_TRUNC('day', source.valid_from)                                  AS team_data_valid_from,
    COALESCE(DATE_TRUNC('day', source.valid_to), {{ var('tomorrow') }})   AS team_data_valid_to
  FROM source

),


team_superior_groups AS (

  SELECT
    team_id,
    team_name,
    team_superior_team_id,
    COALESCE(team_superior_team_id, '')                                                                                   AS no_null_superior,
    LAG(no_null_superior, 1, '') OVER (PARTITION BY team_id ORDER BY team_data_valid_from)                                AS lag_superior_id,
    CONDITIONAL_TRUE_EVENT(no_null_superior != lag_superior_id) OVER (PARTITION BY team_id ORDER BY team_data_valid_from) AS superior_id_group,
    team_data_valid_from,
    team_data_valid_to
  FROM team_data

),


team_superior_change AS (

  SELECT
    team_id,
    team_name,
    team_superior_team_id,
    superior_id_group,
    MIN(team_data_valid_from) AS valid_from,
    MAX(team_data_valid_to)   AS valid_to
  FROM team_superior_groups
  GROUP BY 1, 2, 3, 4

),

recursive_hierarchy AS (

  SELECT
    root.team_id               AS team_id,
    root.team_name             AS team_name,
    root.team_superior_team_id AS team_superior_team_id,
    root.superior_id_group     AS superior_id_group,
    root.valid_from            AS valid_from,
    root.valid_to              AS valid_to,
    TO_ARRAY(root.valid_from)  AS valid_from_list,
    TO_ARRAY(root.valid_to)    AS valid_to_list,
    TO_ARRAY(root.team_id)     AS upstream_organizations,
    TO_ARRAY(root.team_name)   AS upstream_organization_names
  FROM team_superior_change AS root
  WHERE team_superior_team_id IS NULL

  UNION ALL

  SELECT
    iter.team_id                                                     AS team_id,
    iter.team_name                                                   AS team_name,
    iter.team_superior_team_id                                       AS team_superior_team_id,
    iter.superior_id_group     										                   AS superior_id_group,
    iter.valid_from                                                  AS valid_from,
    iter.valid_to                                                    AS valid_to,
    ARRAY_APPEND(anchor.valid_from_list, iter.valid_from)            AS valid_from_list,
    ARRAY_APPEND(anchor.valid_to_list, iter.valid_to)                AS valid_to_list,
    ARRAY_APPEND(anchor.upstream_organizations, iter.team_id)        AS upstream_organizations,
    ARRAY_APPEND(anchor.upstream_organization_names, iter.team_name) AS upstream_organization_names
  FROM recursive_hierarchy AS anchor
  INNER JOIN team_superior_change AS iter
    ON iter.team_superior_team_id = anchor.team_id
      AND NOT ARRAY_CONTAINS(iter.team_id::VARIANT, anchor.upstream_organizations)
      /*
      When joining two SCD we want to join any time the two date ranges overlap and
      with the timestamps being truncated to the day a the overlaps must be specific
      */
      AND (
        CASE
          WHEN iter.valid_from >= anchor.valid_from AND iter.valid_from < anchor.valid_to THEN TRUE
          WHEN iter.valid_to > anchor.valid_from AND iter.valid_to <= anchor.valid_to THEN TRUE
          WHEN anchor.valid_from >= iter.valid_from AND anchor.valid_from < iter.valid_to THEN TRUE
          ELSE FALSE
        END) = TRUE

),

team_hierarchy AS (

  SELECT
    recursive_hierarchy.team_id                                                 AS team_id,
    recursive_hierarchy.team_superior_team_id                                   AS team_superior_team_id,
    COALESCE(recursive_hierarchy.upstream_organizations[0]::VARCHAR, '--')      AS hierarchy_level_1,
    COALESCE(recursive_hierarchy.upstream_organizations[1]::VARCHAR, '--')      AS hierarchy_level_2,
    COALESCE(recursive_hierarchy.upstream_organizations[2]::VARCHAR, '--')      AS hierarchy_level_3,
    COALESCE(recursive_hierarchy.upstream_organizations[3]::VARCHAR, '--')      AS hierarchy_level_4,
    COALESCE(recursive_hierarchy.upstream_organizations[4]::VARCHAR, '--')      AS hierarchy_level_5,
    COALESCE(recursive_hierarchy.upstream_organizations[5]::VARCHAR, '--')      AS hierarchy_level_6,
    COALESCE(recursive_hierarchy.upstream_organizations[6]::VARCHAR, '--')      AS hierarchy_level_7,
    COALESCE(recursive_hierarchy.upstream_organizations[7]::VARCHAR, '--')      AS hierarchy_level_8,
    COALESCE(recursive_hierarchy.upstream_organizations[8]::VARCHAR, '--')      AS hierarchy_level_9,
    COALESCE(recursive_hierarchy.upstream_organization_names[0]::VARCHAR, '--') AS hierarchy_level_1_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[1]::VARCHAR, '--') AS hierarchy_level_2_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[2]::VARCHAR, '--') AS hierarchy_level_3_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[3]::VARCHAR, '--') AS hierarchy_level_4_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[4]::VARCHAR, '--') AS hierarchy_level_5_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[5]::VARCHAR, '--') AS hierarchy_level_6_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[6]::VARCHAR, '--') AS hierarchy_level_7_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[7]::VARCHAR, '--') AS hierarchy_level_8_name,
    COALESCE(recursive_hierarchy.upstream_organization_names[8]::VARCHAR, '--') AS hierarchy_level_9_name,
    recursive_hierarchy.upstream_organizations                                  AS hierarchy_levels_array,
    /*
      Add the superior_id_group (one of the keys used to group the 
      hierarchy changes in team_superior_change) to make the grouping unique
    */
    recursive_hierarchy.superior_id_group                                       AS superior_id_group,
    ARRAY_MAX(recursive_hierarchy.valid_from_list)                              AS hierarchy_valid_from,
    ARRAY_MIN(recursive_hierarchy.valid_to_list)                                AS hierarchy_valid_to
  FROM recursive_hierarchy
  WHERE hierarchy_valid_to > hierarchy_valid_from

),

final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['team_data.team_id']) }}                        AS dim_team_sk,
    team_data.team_id,
    team_data.team_superior_team_id,
    team_hierarchy.hierarchy_level_1,
    team_hierarchy.hierarchy_level_2,
    team_hierarchy.hierarchy_level_3,
    team_hierarchy.hierarchy_level_4,
    team_hierarchy.hierarchy_level_5,
    team_hierarchy.hierarchy_level_6,
    team_hierarchy.hierarchy_level_7,
    team_hierarchy.hierarchy_level_8,
    team_hierarchy.hierarchy_level_9,
    team_hierarchy.hierarchy_level_1_name,
    team_hierarchy.hierarchy_level_2_name,
    team_hierarchy.hierarchy_level_3_name,
    team_hierarchy.hierarchy_level_4_name,
    team_hierarchy.hierarchy_level_5_name,
    team_hierarchy.hierarchy_level_6_name,
    team_hierarchy.hierarchy_level_7_name,
    team_hierarchy.hierarchy_level_8_name,
    team_hierarchy.hierarchy_level_9_name,
    team_hierarchy.hierarchy_levels_array,
    team_data.team_hierarchy_level                                                AS team_hierarchy_level,
    team_data.is_team_inactivated,
    team_data.team_name,
    team_data.team_manager_name,
    team_data.team_manager_name_id,
    team_data.team_inactivated_date,
    team_data.team_members_count,
    team_data.is_team_manager_inherited,
    IFF(team_data.team_inactivated_date IS NULL, TRUE, FALSE)                     AS is_team_active,
    GREATEST(team_hierarchy.hierarchy_valid_from, team_data.team_data_valid_from) AS valid_from,
    LEAST(team_hierarchy.hierarchy_valid_to, team_data.team_data_valid_to)        AS valid_to,
    IFF(DATE(valid_to) = {{ var('tomorrow') }}, TRUE, FALSE)                      AS is_current
  FROM team_data
  LEFT JOIN team_hierarchy
    ON team_hierarchy.team_id = team_data.team_id
      AND NOT (team_data.team_data_valid_to <= team_hierarchy.hierarchy_valid_from
          OR team_data.team_data_valid_from >= team_hierarchy.hierarchy_valid_to)

)


{{ dbt_audit(
    cte_ref="final",
    created_by="@lisvinueza",
    updated_by="@michellecooper",
    created_date="2023-01-17",
    updated_date="2023-10-18",
 	) }}

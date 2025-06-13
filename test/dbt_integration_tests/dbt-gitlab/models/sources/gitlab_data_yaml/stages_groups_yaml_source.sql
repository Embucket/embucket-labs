WITH source AS (

    SELECT *
    FROM {{ ref('stages_yaml_source') }}

), groups_exploded AS (

    SELECT
      {{ dbt_utils.star(from=ref('stages_yaml_source'), except=["STAGE_GROUPS"]) }},
      d.value AS data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(stage_groups::VARIANT)[0]) d

), groups_parsed_out AS (

    SELECT 
      {{ dbt_utils.star(from=ref('stages_yaml_source'), except=["STAGE_GROUPS"]) }},
      data_by_row['name']::VARCHAR              AS group_name,
      data_by_row['sets']::ARRAY                AS group_sets,
      data_by_row['pm']::VARCHAR                AS group_project_manager,
      data_by_row['pdm']::ARRAY                 AS group_pdm,
      data_by_row['ux']::ARRAY                  AS group_ux,
      data_by_row['uxr']::ARRAY                 AS group_uxr,
      data_by_row['tech_writer']::VARCHAR       AS group_tech_writer,
      data_by_row['tw_backup']::VARCHAR         AS group_tech_writer_backup,
      data_by_row['appsec_engineer']::VARCHAR   AS group_appsec_engineer
    FROM groups_exploded

)

SELECT *
FROM groups_parsed_out
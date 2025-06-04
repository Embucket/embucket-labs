WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'stages') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext['stages']), OUTER => TRUE) d

), intermediate_stage AS (

    SELECT
      rank,
      data_by_row['related']::ARRAY                          AS related_stages,
      snapshot_date,
      data_by_row['pm']::VARCHAR                             AS stage_product_manager,
      data_by_row['display_name']::VARCHAR                   AS stage_display_name,
      data_by_row['established']::NUMBER                     AS stage_year_established,
      data_by_row['lifecycle']::NUMBER                       AS stage_lifecycle,
      data_by_row['horizon']::VARCHAR                        AS stage_horizon,
      data_by_row['contributions']::NUMBER                   AS stage_contributions,
      data_by_row['usage_driver_score']::NUMBER              AS stage_usage_driver_score,
      data_by_row['sam_driver_score']::NUMBER                AS stage_sam_driver_score,
      data_by_row['stage_development_spend_percent']::NUMBER AS stage_development_spend_percent,
      data_by_row['groups']::ARRAY                           AS stage_groups,
      data_by_row['section']::VARCHAR                        AS stage_section
    FROM intermediate
      
)

SELECT *
FROM intermediate_stage

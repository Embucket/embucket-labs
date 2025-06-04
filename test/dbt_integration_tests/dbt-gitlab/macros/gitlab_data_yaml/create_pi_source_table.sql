{%- macro create_pi_source_table(source_performance_indicator) -%}

WITH source AS (

    SELECT *
    FROM {{ source_performance_indicator }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

     SELECT 
      data_by_row['name']::VARCHAR                         AS pi_name,
      data_by_row['org']::VARCHAR                          AS org_name,
      data_by_row['definition']::VARCHAR                   AS pi_definition,
      data_by_row['is_key']::BOOLEAN                       AS is_key,
      data_by_row['is_primary']::BOOLEAN                   AS is_primary,
      data_by_row['public']::BOOLEAN                       AS is_public,
      data_by_row['sisense_data'] IS NOT NULL              AS is_embedded,
      data_by_row['target']::VARCHAR                       AS pi_target,
      data_by_row['target_name']::VARCHAR                  AS pi_metric_target_name,
      data_by_row['monthly_recorded_targets']::VARCHAR     AS pi_monthly_recorded_targets,
      data_by_row['monthly_estimated_targets']::VARCHAR    AS pi_monthly_estimated_targets,
      data_by_row['metric_name']::VARCHAR                  AS pi_metric_name,
      data_by_row['telemetry_type']::VARCHAR               AS telemetry_type,
      data_by_row['urls']::VARCHAR                         AS pi_url,
      data_by_row['sisense_data'].chart::VARCHAR           AS sisense_chart_id,
      data_by_row['sisense_data'].dashboard::VARCHAR       AS sisense_dashboard_id,
      snapshot_date
    FROM intermediate

), intermediate_stage AS (

    SELECT 
      {{ dbt_utils.generate_surrogate_key(['pi_name', 'org_name', 'pi_definition','is_key','is_public','is_embedded','pi_target', 'pi_metric_target_name', 'pi_monthly_recorded_targets', 'pi_monthly_estimated_targets', 'pi_url']) }} AS unique_key,
      renamed.*
    FROM renamed

), final AS (

    SELECT *,
      FIRST_VALUE(snapshot_date) OVER (PARTITION BY pi_name ORDER BY snapshot_date) AS date_first_added, 
      MIN(snapshot_date) OVER (PARTITION BY unique_key ORDER BY snapshot_date)      AS valid_from_date,
      MAX(snapshot_date) OVER (PARTITION BY unique_key ORDER BY snapshot_date DESC) AS valid_to_date
    FROM intermediate_stage

)

SELECT *
FROM final


{% endmacro %}

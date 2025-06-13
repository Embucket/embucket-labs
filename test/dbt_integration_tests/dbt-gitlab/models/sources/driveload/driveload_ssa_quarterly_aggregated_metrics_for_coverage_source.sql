WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'ssa_quarterly_aggregated_metrics_for_coverage') }}

)

SELECT
  agg_key_value::VARCHAR                            AS agg_key_value,
  metric_name::VARCHAR                              AS metric_name,
  close_day_of_fiscal_quarter_normalised::NUMBER    AS close_day_of_fiscal_quarter_normalised,
  close_fiscal_quarter_name::VARCHAR                AS close_fiscal_quarter_name,
  metric_value::FLOAT                               AS metric_value,
  total_booked_net_arr::FLOAT                       AS total_booked_net_arr,
  booked_net_arr::FLOAT                             AS booked_net_arr,
  metric_coverage::FLOAT                            AS metric_coverage,
  agg_key_name::VARCHAR                             AS agg_key_name

FROM source

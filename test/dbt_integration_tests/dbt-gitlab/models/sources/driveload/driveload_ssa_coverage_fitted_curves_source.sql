WITH source AS (

    SELECT *
    FROM {{ source('driveload', 'ssa_coverage_fitted_curves') }}

)

SELECT
  key_agg_day::VARCHAR                             AS key_agg_day,
  agg_key_name::VARCHAR                            AS agg_key_name,
  agg_key_value::VARCHAR                           AS agg_key_value,
  close_day_of_fiscal_quarter_normalised::FLOAT    AS close_day_of_fiscal_quarter_normalised,
  bookings_linearity::FLOAT                        AS bookings_linearity,
  open_1plus_net_arr_coverage::FLOAT               AS open_1plus_net_arr_coverage,
  open_3plus_net_arr_coverage::FLOAT               AS open_3plus_net_arr_coverage,
  open_4plus_net_arr_coverage::FLOAT               AS open_4plus_net_arr_coverage,
  rq_plus_1_open_1plus_net_arr_coverage::FLOAT     AS rq_plus_1_open_1plus_net_arr_coverage,
  rq_plus_1_open_3plus_net_arr_coverage::FLOAT     AS rq_plus_1_open_3plus_net_arr_coverage,
  rq_plus_1_open_4plus_net_arr_coverage::FLOAT     AS rq_plus_1_open_4plus_net_arr_coverage,
  rq_plus_2_open_1plus_net_arr_coverage::FLOAT     AS rq_plus_2_open_1plus_net_arr_coverage,
  rq_plus_2_open_3plus_net_arr_coverage::FLOAT     AS rq_plus_2_open_3plus_net_arr_coverage,
  rq_plus_2_open_4plus_net_arr_coverage::FLOAT     AS rq_plus_2_open_4plus_net_arr_coverage
  
FROM source

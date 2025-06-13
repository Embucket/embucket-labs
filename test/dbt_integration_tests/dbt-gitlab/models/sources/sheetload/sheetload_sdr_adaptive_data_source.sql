WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'sdr_adaptive_data') }}

), renamed as (

    SELECT
      current_month::DATE                   AS current_month,
      name::VARCHAR                         AS name,
      start_month::DATE                     AS start_month,
      add_internal_accounted::VARCHAR       AS add_internal_accounted,
      hiring_manager::VARCHAR               AS hiring_manager,
      role::VARCHAR                         AS role,
      region::VARCHAR                       AS region,
      segment::VARCHAR                      AS segment,
      ghpid::NUMBER                         AS GHPID,
      employment::VARCHAR                   AS employment,
      status::VARCHAR                       AS status,
      weighting::NUMBER                     AS weighting,
      months_tenure::NUMBER                 AS months_tenure,
      first_2_months::NUMBER                AS first_2_months,
      ramping::VARCHAR                      AS ramping,
      ramp_cutoff_date::DATE                AS ramp_cutoff_date,
      start_date_assumption::DATE           AS start_date_assumption
    FROM source
)

SELECT *
FROM renamed

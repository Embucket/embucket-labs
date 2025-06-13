WITH snapshot_dates AS (
    -- Use the 8th calendar day up to 2024-03-01 and 5th calendar day after
    SELECT DISTINCT
      first_day_of_month,
      CASE WHEN first_day_of_month < '2024-03-01'
        THEN snapshot_date_fpa
      ELSE snapshot_date_fpa_fifth 
      END                                   AS snapshot_date_fpa
    FROM {{ ref('dim_date') }}

), mart_available_to_renew_snapshot AS (

    SELECT *
    FROM {{ ref('mart_available_to_renew_snapshot_model') }}

), final AS (

    SELECT 
      mart_available_to_renew_snapshot.*,
      snapshot_dates.first_day_of_month
    FROM mart_available_to_renew_snapshot
    INNER JOIN snapshot_dates
      ON mart_available_to_renew_snapshot.snapshot_date = snapshot_dates.snapshot_date_fpa

)

SELECT *
FROM final

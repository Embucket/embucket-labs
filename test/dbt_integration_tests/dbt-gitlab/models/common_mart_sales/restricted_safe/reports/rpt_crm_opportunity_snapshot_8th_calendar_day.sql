WITH snapshot_dates AS (
    --Use the 8th calendar day to snapshot ATR
    SELECT DISTINCT
      first_day_of_month,
      snapshot_date_fpa
    FROM {{ ref('dim_date') }}
    ORDER BY 1 DESC

), crm_opportunity_daily_snapshot AS (

    SELECT *
    FROM {{ ref('mart_crm_opportunity_daily_snapshot') }}

), final AS (

    SELECT *
    FROM crm_opportunity_daily_snapshot
    INNER JOIN snapshot_dates
      ON crm_opportunity_daily_snapshot.snapshot_date = snapshot_dates.snapshot_date_fpa

)

SELECT *
FROM final

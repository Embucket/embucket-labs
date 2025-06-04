WITH zuora_subscriptions_new AS (

    SELECT *
    FROM {{ ref('zuora_subscriptions_new') }}

), zuora_subscriptions_updated AS (

    SELECT *
    FROM {{ ref('zuora_subscriptions_updated') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), final AS (

    SELECT
      dates.date_day            AS date_day,
      new_records.rowcount      AS new_records,
      updated_records.rowcount  AS updated_records
    FROM dim_date dates
    LEFT JOIN zuora_subscriptions_new new_records ON new_records.date_day = dates.date_day
    LEFT JOIN zuora_subscriptions_updated updated_records ON updated_records.date_day = dates.date_day
    WHERE (new_records.rowcount > 0 OR updated_records.rowcount > 0)

)

SELECT *
FROM final

WITH zuora_accounts_new AS (

    SELECT *
    FROM {{ ref('zuora_accounts_new') }}

), zuora_accounts_updated AS (

    SELECT *
    FROM {{ ref('zuora_accounts_updated') }}

), dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

), final AS (

    SELECT
      dates.date_day            AS date_day,
      new_records.rowcount      AS new_records,
      updated_records.rowcount  AS updated_records
    FROM dim_date dates
    LEFT JOIN zuora_accounts_new new_records ON new_records.date_day = dates.date_day
    LEFT JOIN zuora_accounts_updated updated_records ON updated_records.date_day = dates.date_day
    WHERE (new_records.rowcount > 0 OR updated_records.rowcount > 0)

)

SELECT *
FROM final

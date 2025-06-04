{{ config({
        "materialized": "incremental",
        "unique_key": "mart_retention_parent_account_snapshot_id",
        "tags": ["edm_snapshot", "retention_snapshots"]
    })
}}

/* grain: one record per subscription, product per month */
WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}

), mart_retention_parent_account AS (

    SELECT
      *
    FROM {{ ref('prep_mart_retention_parent_account_snapshot_base') }}

), mart_retention_parent_account_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      mart_retention_parent_account.*
    FROM mart_retention_parent_account
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= mart_retention_parent_account.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('mart_retention_parent_account.dbt_valid_to') }}

), final AS (

    SELECT
     {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'fct_retention_id']) }} AS mart_retention_parent_account_snapshot_id,
       *
    FROM mart_retention_parent_account_spined

)


SELECT *
FROM final

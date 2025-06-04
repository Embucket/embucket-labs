{{ config({
        "materialized": "incremental",
        "unique_key": "dim_subscription_snapshot_id",
        "tags": ["edm_snapshot", "subscription_snapshot"]
    })
}}


WITH snapshot_dates AS (

   SELECT *
   FROM {{ ref('dim_date') }}
   WHERE date_actual >= '2020-03-01' and date_actual <= CURRENT_DATE
   {% if is_incremental() %}

   -- this filter will only be applied on an incremental run
   AND date_id > (SELECT max(snapshot_id) FROM {{ this }})

   {% endif %}

), dim_subscription AS (

    SELECT
      *
    FROM {{ ref('prep_dim_subscription_snapshot_base') }}

),  dim_subscription_spined AS (

    SELECT
      snapshot_dates.date_id AS snapshot_id,
      dim_subscription.*
    FROM dim_subscription
    INNER JOIN snapshot_dates
      ON snapshot_dates.date_actual >= dim_subscription.dbt_valid_from
      AND snapshot_dates.date_actual < {{ coalesce_to_infinity('dim_subscription.dbt_valid_to') }}

), final AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['snapshot_id', 'dim_subscription_id']) }} AS dim_subscription_snapshot_id,
       *
    FROM dim_subscription_spined

)

SELECT * 
FROM final
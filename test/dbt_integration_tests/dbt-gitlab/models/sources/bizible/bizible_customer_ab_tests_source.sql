WITH source AS (

    SELECT

      visitor_id                AS visitor_id,
      cookie_id                 AS cookie_id,
      event_date                AS event_date,
      modified_date             AS modified_date,
      ip_address                AS ip_address,
      city                      AS city,
      region                    AS region,
      country                   AS country,
      experiment_id             AS experiment_id,
      experiment_name           AS experiment_name,
      variation_id              AS variation_id,
      variation_name            AS variation_name,
      abtest_user_id            AS abtest_user_id,
      is_deleted                AS is_deleted,
      _created_date             AS _created_date,
      _modified_date            AS _modified_date,
      _deleted_date             AS _deleted_date

    FROM {{ source('bizible', 'biz_customer_ab_tests') }}
 
)

SELECT *
FROM source


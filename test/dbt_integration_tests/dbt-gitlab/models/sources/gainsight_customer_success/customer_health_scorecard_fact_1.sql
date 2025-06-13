{{ config(
    tags=["mnpi"]
) }}

WITH source AS (
  SELECT *
  FROM {{ source('gainsight_customer_success','customer_health_scorecard_fact_1') }}
),

renamed AS (

  SELECT
    id::NUMBER                       AS id,
    _fivetran_deleted::BOOLEAN       AS _fivetran_deleted,
    cur_score_id_gc::VARCHAR         AS cur_score_id_gc,
    scorecard_id_gc::VARCHAR         AS scorecard_id_gc,
    comments_gc::VARCHAR             AS comments_gc,
    modified_date::TIMESTAMP         AS modified_date,
    measure_id_gc::VARCHAR           AS measure_id_gc,
    prev_score_id_gc::VARCHAR        AS prev_score_id_gc,
    active::BOOLEAN                  AS active,
    account_id_gc::VARCHAR           AS account_id_gc,
    score_modified_by_id_gc::VARCHAR AS score_modified_by_id_gc,
    created_date::TIMESTAMP          AS created_date,
    trend_gc::VARCHAR                AS trend_gc,
    modified_by_id_gc::VARCHAR       AS modified_by_id_gc,
    score_modified_at_gc::VARCHAR    AS score_modified_at_gc,
    created_by_id_gc::VARCHAR        AS created_by_id_gc,
    _fivetran_synced::TIMESTAMP      AS _fivetran_synced
  FROM source
)

SELECT *
FROM renamed

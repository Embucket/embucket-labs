WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'churn_forecasting_scores') }}

), intermediate AS (

    SELECT
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT

      data_by_row['oldest_subscription_in_cohort']::VARCHAR                AS oldest_subscription_in_cohort,
      data_by_row['dim_crm_opportunity_id_current_open_renewal']::VARCHAR  AS dim_crm_opportunity_id_current_open_renewal,
      data_by_row['score_date']::TIMESTAMP                                 AS score_date,
      data_by_row['renewal_date']::TIMESTAMP                               AS renewal_date,
      data_by_row['current_arr']::NUMBER(38,2)                             AS current_arr,
      data_by_row['churn_score']::NUMBER(38,4)                             AS churn_score,
      data_by_row['contraction_score']::NUMBER(38,4)                       AS contraction_score,
      data_by_row['outcome']::VARCHAR                                      AS outcome,
      data_by_row['arr_expected_to_renew']::NUMBER(38,2)                   AS arr_expected_to_renew,
      data_by_row['model_version']::FLOAT                                  AS model_version,
      uploaded_at::TIMESTAMP                                               AS uploaded_at

    FROM intermediate

)
SELECT *
FROM parsed

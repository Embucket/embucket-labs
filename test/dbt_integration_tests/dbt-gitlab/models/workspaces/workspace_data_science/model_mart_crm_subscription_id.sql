{{ config(
     materialized = "table"
) }}

WITH churn_forecasting AS (

    SELECT

        oldest_subscription_in_cohort                   as oldest_subscription_in_cohort,
        dim_crm_opportunity_id_current_open_renewal     as dim_crm_opportunity_id_current_open_renewal,
        score_date                                      as score_date,
        renewal_date                                    as renewal_date,
        current_arr                                     as current_arr,
        churn_score                                     as churn_score,
        contraction_score                               as contraction_score,
        outcome                                         as outcome,
        arr_expected_to_renew                           as arr_expected_to_renew

    FROM {{ ref('churn_forecasting_scores') }}

)

SELECT *
FROM churn_forecasting

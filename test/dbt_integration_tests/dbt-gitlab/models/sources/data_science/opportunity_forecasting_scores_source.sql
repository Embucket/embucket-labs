WITH source AS (

    SELECT
        dim_crm_opportunity_id,
        score_date,
        forecasted_days_remaining,
        forecasted_close_won_date,
        forecasted_close_won_quarter,
        score_group,
        insights,
        submodel,
        model_version,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM {{ source('data_science', 'opportunity_forecasting_scores') }}

)

SELECT *
FROM source
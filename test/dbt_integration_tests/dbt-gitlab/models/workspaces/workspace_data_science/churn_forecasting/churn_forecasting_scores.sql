WITH source AS (

    SELECT *
    FROM {{ ref('churn_forecasting_scores_source') }}

)

SELECT *
FROM source
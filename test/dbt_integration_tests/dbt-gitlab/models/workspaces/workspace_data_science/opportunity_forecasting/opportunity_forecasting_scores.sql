WITH source AS (

    SELECT *
    FROM {{ ref('opportunity_forecasting_scores_source') }}

)

SELECT *
FROM source
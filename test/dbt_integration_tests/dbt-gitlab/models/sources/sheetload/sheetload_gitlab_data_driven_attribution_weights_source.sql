WITH source AS (

        SELECT
                channel::TEXT AS channel,
                offer_type::TEXT AS offer_type,
                weight::FLOAT AS weight
        FROM {{ source('sheetload','gitlab_data_driven_attribution_weights') }}

        )
        SELECT * 
        FROM source

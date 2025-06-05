WITH source AS (

    SELECT
        lead_id,
        score_date,
        score,
        decile,
        score_group,
        insights,
        submodel,
        model_version,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM EMBUCKET.data_science.ptpl_scores
)

SELECT *
FROM source
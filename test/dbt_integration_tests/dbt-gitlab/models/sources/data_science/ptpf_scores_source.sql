WITH source AS (

    SELECT
        namespace_id,
        score_date,
        score,
        decile,
        score_group,
        insights,
        sub_model,
        days_since_trial_start,
        model_version,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM {{ source('data_science', 'ptpf_scores') }}
)

SELECT *
FROM source
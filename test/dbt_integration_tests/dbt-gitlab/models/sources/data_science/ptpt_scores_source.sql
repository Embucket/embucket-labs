WITH source AS (

    SELECT
        namespace_id            as namespace_id,
        score_date              as score_date,
        score                   as score,
        decile                  as decile,
        importance              as importance,
        grouping                as score_group,
        insights                as insights,
        model_version           as model_version,
        uploaded_at::TIMESTAMP  as uploaded_at
    FROM {{ source('data_science', 'ptpt_scores') }}
)

SELECT *
FROM source
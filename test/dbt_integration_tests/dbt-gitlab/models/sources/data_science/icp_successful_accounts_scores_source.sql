WITH source AS (

    SELECT
        crm_account_id,
        score_date,
        cluster_id,
        cluster_name,
        icp_group,
        model_version,
        submodel,
        uploaded_at::TIMESTAMP as uploaded_at
    FROM {{ source('data_science', 'icp_successful_accounts_scores') }}
)

SELECT *
FROM source
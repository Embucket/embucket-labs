WITH source AS (

  SELECT *
  FROM {{ source('data_science', 'namespace_segmentation_scores') }}

),

intermediate AS (

  SELECT
    d.value AS data_by_row,
    uploaded_at
  FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => TRUE) AS d

),

parsed AS (

  SELECT
    data_by_row['crm_account_owner_user_segment']::VARCHAR AS crm_account_owner_user_segment,
    data_by_row['dim_crm_account_id']::VARCHAR AS dim_crm_account_id,
    data_by_row['dim_namespace_id']::INT AS dim_namespace_id,
    data_by_row['grouping']::INT AS grouping,
    data_by_row['score_date']::TIMESTAMP AS score_date,
    data_by_row['segmentation']::VARCHAR AS segmentation,
    data_by_row['model_version']::FLOAT  AS model_version,
    CURRENT_TIMESTAMP()::TIMESTAMP AS uploaded_at
  FROM intermediate

)

SELECT *
FROM parsed

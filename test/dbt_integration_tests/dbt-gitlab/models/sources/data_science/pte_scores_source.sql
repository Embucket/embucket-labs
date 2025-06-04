WITH source AS (

    SELECT *
    FROM {{ source('data_science', 'pte_scores') }}

), intermediate AS (

    SELECT
      d.value as data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => PARSE_JSON(jsontext), outer => true) d

), parsed AS (

    SELECT

      data_by_row['crm_account_id']::VARCHAR                AS crm_account_id,
      data_by_row['score_date']::TIMESTAMP                  AS score_date,
      data_by_row['score']::NUMBER(38,4)                    AS score,
      data_by_row['decile']::INT                            AS decile,
      data_by_row['importance']::INT                        AS importance,
      data_by_row['grouping']::INT                          AS score_group,
      data_by_row['insights']::VARCHAR                      AS insights,
      data_by_row['model_version']::FLOAT                   AS model_version,
      uploaded_at::TIMESTAMP                                AS uploaded_at,
      data_by_row['uptier_likely']::BOOLEAN                 AS uptier_likely

    FROM intermediate

)
SELECT *
FROM parsed

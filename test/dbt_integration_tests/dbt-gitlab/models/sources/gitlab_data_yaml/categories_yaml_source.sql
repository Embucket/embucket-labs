WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'categories') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT d.value                            AS data_by_row,
      DATE_TRUNC('day', uploaded_at)::DATE    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['acquisition_appetite']::VARCHAR          AS acquisition_appetite,
      data_by_row['alt_link']::VARCHAR                      AS alt_link,
      TRY_TO_TIMESTAMP(data_by_row['available']::VARCHAR)   AS available,
      data_by_row['body']::VARCHAR                          AS body,
      TRY_TO_TIMESTAMP(data_by_row['complete']::VARCHAR)    AS complete,
      data_by_row['description']::VARCHAR                   AS description,
      data_by_row['direction']::VARCHAR                     AS direction,
      data_by_row['documentation']::VARCHAR                 AS documentation,
      data_by_row['feature_labels']::VARCHAR                AS feature_labels,
      TRY_TO_TIMESTAMP(data_by_row['lovable']::VARCHAR)     AS lovable,
      data_by_row['label']::VARCHAR                         AS label,
      TRY_TO_BOOLEAN(data_by_row['marketing']::VARCHAR)     AS marketing,
      data_by_row['marketing_page']::VARCHAR                AS marketing_page,
      data_by_row['maturity']::VARCHAR                      AS maturity,
      data_by_row['name']::VARCHAR                          AS name,
      TRY_TO_BOOLEAN(data_by_row['new_maturity']::VARCHAR)  AS new_maturity,
      data_by_row['partnership_appetite']::VARCHAR          AS partnership_appetite,
      data_by_row['priority_level']::VARCHAR                AS priority_level,
      data_by_row['roi']::VARCHAR                           AS roi,
      data_by_row['stage']::VARCHAR                         AS stage,
      TRY_TO_TIMESTAMP(data_by_row['viable']::VARCHAR)      AS viable,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM renamed

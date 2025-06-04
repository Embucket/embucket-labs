WITH source AS (

    SELECT *,
        RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'team') }}
    ORDER BY uploaded_at DESC

), intermediate AS (

    SELECT d.value                          AS data_by_row,
    date_trunc('day', uploaded_at)::DATE    AS snapshot_date,
    rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), renamed AS (

    SELECT
      data_by_row['departments']::ARRAY     AS departments,
      data_by_row['gitlab']::VARCHAR        AS gitlab_username,
      data_by_row['name']::VARCHAR          AS name,
      data_by_row['projects']::VARCHAR      AS projects,
      data_by_row['slug']::VARCHAR          AS yaml_slug,
      data_by_row['type']::VARCHAR          AS type,
      snapshot_date,
      rank
    FROM intermediate

)

SELECT *
FROM renamed
WITH source AS (

    SELECT *,
      RANK() OVER (PARTITION BY DATE_TRUNC('day', uploaded_at) ORDER BY uploaded_at DESC) AS rank
    FROM {{ source('gitlab_data_yaml', 'releases') }}

), intermediate AS (

    SELECT
      d.value                                 AS data_by_row,
      date_trunc('day', uploaded_at)::date    AS snapshot_date,
      rank
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => TRUE) d

), intermediate_stage AS (

    SELECT
      data_by_row['version']::VARCHAR                           AS major_minor_version,
      SPLIT_PART(major_minor_version, '.', 1)                   AS major_version,
      SPLIT_PART(major_minor_version, '.', 2)                   AS minor_version,
      TRY_TO_DATE(data_by_row['date']::VARCHAR)                 AS release_date,
      data_by_row['manager_americas'][0]::VARCHAR               AS release_manager_americas,
      data_by_row['manager_apac_emea'][0]::VARCHAR              AS release_manager_emea,
      rank,
      snapshot_date
    FROM intermediate
      
)

SELECT *
FROM intermediate_stage

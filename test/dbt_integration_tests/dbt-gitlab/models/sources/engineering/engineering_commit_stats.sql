WITH source AS (

    SELECT *
    FROM {{ source('engineering', 'commit_stats') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), intermediate AS (

    SELECT d.value as data_by_row
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), renamed AS (

    SELECT
      data_by_row['backendCoverage']::float           AS backend_coverage,
      data_by_row['backendCoverageAbsolute']::NUMBER  AS backend_coverage_absolute,
      data_by_row['backendCoverageTotal']::NUMBER     AS backend_coverage_total,
      data_by_row['commitDate']::date                 AS commit_date,
      data_by_row['jestCoverage']::float              AS jest_coverage,
      data_by_row['jestCoverageAbsolute']::NUMBER     AS jest_coverage_absolute,
      data_by_row['jestCoverageTotal']::NUMBER        AS jest_coverage_total,
      data_by_row['karmaCoverage']::float             AS karma_coverage,
      data_by_row['karmaCoverageAbsolute']::NUMBER    AS karma_coverage_absolute,
      data_by_row['karmaCoverageTotal']::NUMBER       AS karma_coverage_total
    FROM intermediate

)

SELECT *
FROM renamed

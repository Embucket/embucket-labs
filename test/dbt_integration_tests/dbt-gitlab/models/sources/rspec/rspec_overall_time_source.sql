WITH source AS (

    SELECT *
    FROM {{ source('rspec', 'overall_time') }}

), renamed AS (

    SELECT
      commit_hash::VARCHAR                  AS commit,
      commit_time::TIMESTAMP_TZ             AS commit_at_time,
      total_time::FLOAT                     AS total_time_taken_seconds,
      number_of_tests::FLOAT                AS number_of_tests,
      time_per_single_test::FLOAT           AS time_per_single_test_seconds,
      total_queries::FLOAT                  AS total_queries,
      total_query_time::FLOAT               AS total_query_time_seconds,
      total_requests::FLOAT                 AS total_requests,
      _UPDATED_AT::FLOAT                    AS updated_at
    FROM source

)

SELECT *
FROM renamed

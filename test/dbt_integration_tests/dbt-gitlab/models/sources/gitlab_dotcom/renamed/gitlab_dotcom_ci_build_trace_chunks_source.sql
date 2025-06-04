WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_build_trace_chunks_dedupe_source') }}

), renamed AS (

    SELECT
      build_id::NUMBER     AS ci_build_id,
      chunk_index::VARCHAR  AS chunk_index,
      data_store::VARCHAR   AS data_store

    FROM source

)


SELECT *
FROM renamed

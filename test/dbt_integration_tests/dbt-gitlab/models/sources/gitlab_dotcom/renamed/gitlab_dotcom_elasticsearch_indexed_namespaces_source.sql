WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_elasticsearch_indexed_namespaces_dedupe_source') }}

), types_cast AS (

    SELECT
      namespace_id::NUMBER            AS namespace_id,
      created_at::TIMESTAMP           AS created_at,
      updated_at::TIMESTAMP           AS updated_at,
      _uploaded_at::NUMBER::TIMESTAMP AS uploaded_at,
      _task_instance::VARCHAR         AS task_instance_name
    FROM source

)

SELECT *
FROM types_cast

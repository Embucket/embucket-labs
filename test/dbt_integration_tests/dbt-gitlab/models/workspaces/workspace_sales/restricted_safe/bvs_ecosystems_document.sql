{{ config({
        "materialized": "table",
        "transient": false
    })
}}

WITH base AS (

  SELECT * EXCLUDE collaborators
  FROM {{ ref("ecosystems_document_source") }}

)

  SELECT *
  FROM base
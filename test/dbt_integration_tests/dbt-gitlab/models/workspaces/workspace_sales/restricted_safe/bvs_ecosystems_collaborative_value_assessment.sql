{{ config({
        "materialized": "table",
        "transient": false
    })
}}

WITH base AS (

  SELECT *
  FROM {{ ref("ecosystems_collaborative_value_assessment_source") }}

)

  SELECT *
  FROM base
{{ config({
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

  SELECT *
  FROM {{ ref('sensitive_queries_details') }}
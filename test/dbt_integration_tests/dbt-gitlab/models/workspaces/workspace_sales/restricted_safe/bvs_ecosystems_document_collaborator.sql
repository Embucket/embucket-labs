{{ config({
        "materialized": "table",
        "transient": false
    })
}}


WITH base AS (

  SELECT document_id,
         collaborators
  FROM {{ ref("ecosystems_document_source") }}

), extracted AS (

  SELECT document_id                   AS document_id,
         try_parse_json(collaborators) AS collaborators
    FROM base

), formatted AS (

  SELECT document_id                      AS document_id,
         VALUE:"id"::NUMBER               AS collaborator_id,
         VALUE:"accessLevel"::VARCHAR     AS access_level,
         VALUE:"documentVisible"::BOOLEAN AS is_document_visible,
         VALUE:"userId"::NUMBER           as user_id,
         VALUE:"username"::VARCHAR        as user_name
  FROM extracted,
  LATERAL FLATTEN(INPUT => collaborators)

)

  SELECT *
  FROM formatted
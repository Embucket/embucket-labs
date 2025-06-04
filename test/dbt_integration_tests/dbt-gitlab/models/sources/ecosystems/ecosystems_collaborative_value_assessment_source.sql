
WITH base AS (

  SELECT *
  FROM {{ source('ecosystems', 'cva') }}

), extracted AS (

  SELECT try_parse_json(jsontext) AS json_text,
         uploaded_at              AS uploaded_at
    FROM base

), formatted AS (

  SELECT VALUE:"Account"::VARCHAR                       AS account_name,
         VALUE:"Active External Collaborators"::NUMBER  AS active_external_collaborator,
         VALUE:"Active Internal Collaborators"::NUMBER  AS active_internal_collaborator,
         VALUE:"Active Partner Collaborators"::NUMBER   AS active_partner_collaborator,
         VALUE:"CRM Opp ID"::VARCHAR                    AS crm_opportunity_id,
         VALUE:"Collaborators"::VARCHAR                 AS collaborators,
         VALUE:"Count of VDs Modified"::NUMBER          AS count_of_value_drivers_modified,
         VALUE:"Discovery Modified"::BOOLEAN            AS is_discovery_modified,
         VALUE:"Document ID"::NUMBER                    AS document_id,
         VALUE:"Effectiveness Score"::FLOAT             AS effectiveness_score,
         VALUE:"Includes Discovery"::BOOLEAN            AS does_include_discovery,
         VALUE:"Investment Modified"::BOOLEAN           AS is_investment_modified,
         VALUE:"Is Demo"::BOOLEAN                       AS is_demo,
         NULLIF(VALUE:"Latest Edit Date",'')::TIMESTAMP AS updated_at,
         VALUE:"Latest Editor"::VARCHAR                 AS latest_editor,
         VALUE:"Model Edits"::NUMBER                    AS models_edits,
         VALUE:"Presentation Name"::VARCHAR             AS presentation_name,
         VALUE:"Viewer Logs"::VARCHAR                   AS viewer_logs,
         uploaded_at::TIMESTAMP                         AS uploaded_at
  FROM extracted,
  LATERAL FLATTEN(INPUT => json_text)

), deduped AS (

  SELECT *
  FROM formatted
  QUALIFY ROW_NUMBER() OVER (PARTITION BY account_name, presentation_name ORDER BY uploaded_at DESC) = 1

)

  SELECT *
  FROM deduped
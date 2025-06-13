
WITH base AS (

  SELECT *
  FROM {{ source('ecosystems', 'document') }}

), extracted AS (

  SELECT try_parse_json(jsontext) AS json_text,
         uploaded_at              AS uploaded_at
    FROM base

), formatted AS (

  SELECT VALUE:"accountName"::VARCHAR                            AS account_name,
         VALUE:"collaborators"                                   AS collaborators,
         VALUE:"crmAccount":"id"::VARCHAR                        AS crm_account_id,
         VALUE:"crmAccount":"name"::VARCHAR                      AS crm_account_name,
         VALUE:"crmAccount":"ownerEmail"::VARCHAR                AS crm_account_owner_email,
         VALUE:"crmAccount":"phone"::VARCHAR                     AS crm_account_phone,
         VALUE:"crmOpportunity":"id"::VARCHAR                    AS crm_opportunity_id,
         VALUE:"crmOpportunity":"name"::VARCHAR                  AS crm_opportunity_name,
         VALUE:"crmOpportunity":"ownerEmail"::VARCHAR            AS crm_opportunity_owner_email,
         VALUE:"crmOpportunity":"phone"::VARCHAR                 AS crm_opportunity_phone,
         VALUE:"demo"::BOOLEAN                                   AS is_demo,
         VALUE:"documentName"::VARCHAR                           AS document_name,
         VALUE:"documentStatus"::VARCHAR                         AS document_status,
         VALUE:"id"::NUMBER                                      AS document_id,
         VALUE:"updateHistory":"createdAt"::DATE                AS update_history_created_at,
         VALUE:"updateHistory":"createdById"::VARCHAR           AS update_history_created_by_id,
         VALUE:"updateHistory":"createdByUsername"::VARCHAR     AS update_history_created_by_username,
         VALUE:"updateHistory":"lastUpdatedAt"::DATE            AS update_history_last_updated_at,
         VALUE:"updateHistory":"lastUpdatedById"::VARCHAR       AS update_history_updated_by_id,
         VALUE:"updateHistory":"lastUpdatedByUsername"::VARCHAR AS update_history_updated_by_username,
         uploaded_at::TIMESTAMP                                  AS uploaded_at
  FROM extracted,
  LATERAL FLATTEN(INPUT => json_text)

), deduped AS (

  SELECT *
  FROM formatted
  QUALIFY ROW_NUMBER() OVER (PARTITION BY document_id ORDER BY uploaded_at DESC) = 1

)

  SELECT *
  FROM deduped
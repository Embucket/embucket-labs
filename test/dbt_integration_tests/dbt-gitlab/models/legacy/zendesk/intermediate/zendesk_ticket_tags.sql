{{config({
    "schema": "legacy"
  })
}}
WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_tickets_source') }}

), renamed AS (

    -- Parse lists to values
    SELECT
      ticket_id     AS ticket_id,
      f.VALUE       AS ticket_tag
    FROM
      source,
    TABLE(FLATTEN(INPUT => STRTOK_TO_ARRAY(ticket_tags, '[], "'))) f

)

SELECT *
FROM renamed

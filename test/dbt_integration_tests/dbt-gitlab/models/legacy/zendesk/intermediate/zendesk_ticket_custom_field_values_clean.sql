{{config({
    "schema": "legacy"
  })
}}

WITH zendesk_custom_fields AS (

    SELECT *
    FROM {{ref('zendesk_ticket_custom_field_values_sensitive')}}

), filtered AS (

    SELECT *
    FROM zendesk_custom_fields
    WHERE ticket_custom_field_id = 360020421853 --Transactions Issue Type

)

SELECT *
FROM filtered

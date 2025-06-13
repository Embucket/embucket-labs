{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    })
}}

WITH zendesk_tickets AS (

    SELECT *
    FROM {{ ref('zendesk_tickets_source') }}

), custom_fields AS (

    SELECT 
      d.value['id']                             AS ticket_custom_field_id,
      REPLACE(d.value['value'], '"', '')        AS ticket_custom_field_value,
      ticket_id                                 AS ticket_id
    FROM zendesk_tickets,
    LATERAL FLATTEN(INPUT => PARSE_JSON(ticket_custom_field_values), outer => true) d
    WHERE ticket_custom_field_value NOT IN ('null', '')

)

SELECT *
FROM custom_fields

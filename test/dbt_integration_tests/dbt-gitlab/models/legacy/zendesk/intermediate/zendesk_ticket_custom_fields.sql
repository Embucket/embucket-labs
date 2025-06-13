{{config({
    "schema": "legacy"
  })
}}

WITH zendesk_ticket_fields AS (

    SELECT *
    FROM {{ref('zendesk_ticket_fields_source')}}

), filtered AS (

    SELECT
      ticket_field_id                        AS ticket_field_id,
      ticket_form_title                      AS ticket_form_title,
      REPLACE(value:"value", '"', '')        AS ticket_field_value,
      REPLACE(VALUE:"name", '"', '')         AS ticket_field_name
    FROM
      zendesk_ticket_fields,
    LATERAL FLATTEN(INPUT => PARSE_JSON(custom_field_options))
)

SELECT *
FROM filtered

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_ticket_audits_dedupe_source') }}
    
),

flattened AS (

    SELECT
      -- primary data
      source.id                                   AS audit_id,
      source.created_at                           AS audit_created_at,
      -- foreign keys
      source.author_id                            AS author_id,
      source.ticket_id                            AS ticket_id,
      -- logical data
      flat_events.value['field_name']             AS audit_field,
      flat_events.value['type']                   AS audit_type,
      flat_events.value['value']                  AS audit_value,
      flat_events.value['id']                     AS audit_event_id

    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(events), OUTER => false) flat_events
    -- currently scoped to only sla_policy and priority
    WHERE flat_events.value['field_name'] IN ('sla_policy', 'priority', 'is_public')
    AND events IS NOT NULL
)

SELECT *
FROM flattened

{{config({
    "schema": "legacy"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_ticket_audits_source') }}
    -- currently scoped to only sla_policy and priority
    WHERE audit_field IN ('sla_policy', 'priority', 'is_public')
    
)

SELECT *
FROM source

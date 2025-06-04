WITH zendesk_ticket_metrics AS (

    SELECT
      ticket_id,
      sla_reply_time_calendar_hours,
      sla_reply_time_business_hours,
      created_at
    FROM {{ref('zendesk_ticket_metrics')}}

), zendesk_ticket_audit_sla AS (

    /* ranking each audit and event within an audit to later select the last
       state of sla_policy and priority prior to first_reply_time */

    SELECT
      ticket_id,
      DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY audit_created_at DESC)      AS sla_audit_rank,
      DENSE_RANK() OVER (PARTITION BY audit_id ORDER BY audit_event_id DESC)         AS sla_audit_event_rank,
      audit_created_at                                                               AS sla_audit_created_at,
      audit_value                                                                    AS sla_policy
    FROM {{ref('zendesk_ticket_audits')}}
    WHERE audit_field = 'sla_policy'

), zendesk_ticket_audit_priority AS (

    SELECT
      ticket_id,
      DENSE_RANK() OVER (PARTITION BY ticket_id ORDER BY audit_created_at DESC)      AS priority_audit_rank,
      DENSE_RANK() OVER (PARTITION BY audit_id ORDER BY audit_event_id DESC)         AS priority_audit_event_rank,
      audit_created_at                                                               AS priority_audit_created_at,
      audit_value                                                                    AS priority
    FROM {{ref('zendesk_ticket_audits')}}
    WHERE audit_field = 'priority'

), zendesk_ticket_emergency_sla_policy AS (

    SELECT
      ticket_id,
      IFF(sla_policy = 'Emergency SLA', TRUE, FALSE) AS is_emergency_sla
    FROM zendesk_ticket_audit_sla
    WHERE sla_audit_rank = 1
    AND sla_audit_event_rank = 1

), zendesk_ticket_reply_time AS (

    SELECT
      zendesk_ticket_metrics.ticket_id,
      zendesk_ticket_metrics.created_at,
      IFF(zendesk_ticket_emergency_sla_policy.is_emergency_sla = FALSE,
          zendesk_ticket_metrics.sla_reply_time_business_hours,
          zendesk_ticket_metrics.sla_reply_time_calendar_hours) AS first_reply_time_sla,
      zendesk_ticket_metrics.sla_reply_time_calendar_hours
    FROM zendesk_ticket_metrics
    LEFT JOIN zendesk_ticket_emergency_sla_policy
      ON zendesk_ticket_metrics.ticket_id = zendesk_ticket_emergency_sla_policy.ticket_id

), zendesk_ticket_sla_metric AS (

    SELECT
      zendesk_ticket_reply_time.*,
      TIMEADD(minute,
        zendesk_ticket_reply_time.sla_reply_time_calendar_hours,
        zendesk_ticket_reply_time.created_at) AS first_reply_at,
      -- Stitch does not send over timestamps of first replies, only duration in minutes
      zendesk_ticket_audit_sla.sla_policy,
      zendesk_ticket_audit_sla.sla_audit_created_at,
      zendesk_ticket_audit_sla.sla_audit_rank,
      zendesk_ticket_audit_sla.sla_audit_event_rank,
      zendesk_ticket_audit_priority.priority,
      zendesk_ticket_audit_priority.priority_audit_created_at,
      zendesk_ticket_audit_priority.priority_audit_rank,
      zendesk_ticket_audit_priority.priority_audit_event_rank
    FROM zendesk_ticket_reply_time
    LEFT JOIN zendesk_ticket_audit_sla
      ON zendesk_ticket_reply_time.ticket_id = zendesk_ticket_audit_sla.ticket_id
    LEFT JOIN zendesk_ticket_audit_priority
      ON zendesk_ticket_reply_time.ticket_id = zendesk_ticket_audit_priority.ticket_id
    WHERE sla_audit_created_at <= first_reply_at
    AND priority_audit_created_at <= first_reply_at

), zendesk_ticket_audit_minimum_ranks AS (

    SELECT
      ticket_id,
      MIN(sla_audit_rank)             AS min_sla_audit_rank,
      MIN(sla_audit_event_rank)       AS min_sla_audit_event_rank,
      MIN(priority_audit_rank)        AS min_priority_audit_rank,
      MIN(priority_audit_event_rank)  AS min_priority_audit_event_rank
        /* minimum rank (latest occurrence) of each SLA policy and priority assignment
        prior to or at the time of first reply */
    FROM zendesk_ticket_sla_metric
    GROUP BY 1

), final AS (

    SELECT DISTINCT
      zendesk_ticket_sla_metric.ticket_id,
      zendesk_ticket_sla_metric.created_at,
      zendesk_ticket_sla_metric.priority,
      zendesk_ticket_sla_metric.sla_policy,
      zendesk_ticket_sla_metric.first_reply_time_sla,
      zendesk_ticket_sla_metric.first_reply_at
    FROM zendesk_ticket_sla_metric
    INNER JOIN zendesk_ticket_audit_minimum_ranks
      ON zendesk_ticket_sla_metric.ticket_id = zendesk_ticket_audit_minimum_ranks.ticket_id
      AND zendesk_ticket_sla_metric.sla_audit_rank = zendesk_ticket_audit_minimum_ranks.min_sla_audit_rank
      AND zendesk_ticket_sla_metric.sla_audit_event_rank = zendesk_ticket_audit_minimum_ranks.min_sla_audit_event_rank
      AND zendesk_ticket_sla_metric.priority_audit_rank = zendesk_ticket_audit_minimum_ranks.min_priority_audit_rank
      AND zendesk_ticket_sla_metric.priority_audit_event_rank = zendesk_ticket_audit_minimum_ranks.min_priority_audit_event_rank

)

SELECT *
FROM final

/*SLA policies and priority per ticket*/
WITH audits AS (

    SELECT *
    FROM {{ref('zendesk_ticket_audits_source')}}
    WHERE audit_field IN ('sla_policy','priority')

),

/*Zendesk ticket data*/
zendesk_tickets AS (

    SELECT *
    FROM {{ref('zendesk_tickets_source')}}

), 

/*Fields taken directly from Zendesk*/
ticket_metrics AS (

    SELECT *
    FROM {{ref('zendesk_ticket_metrics_source')}}

),

/*SLAs per policy*/
slas AS (

    SELECT *
    FROM {{ref('zendesk_sla_policies_source')}}
    WHERE policy_metrics_metric = 'first_reply_time'

),

/*If ticket has an SLA of 'Emergency Support', use that. Otherwise, use the FRT sla policy.*/
tickets_sla AS (

    SELECT audits.ticket_id
    , audit_created_at
    , CASE WHEN audit_value = 'Emergency Support' THEN audit_value ELSE 'Priority Support - FRT' END AS sla_policy
    , DENSE_RANK() OVER (PARTITION BY audits.ticket_id ORDER BY CASE WHEN audit_value = 'Emergency Support' THEN 0 ELSE 1 END, audit_created_at DESC) AS audit_rank
    FROM audits
    WHERE audit_field = 'sla_policy'
    AND audit_value != 'null'

),

/*Choose the priority right before FRT response*/
tickets_priority AS (

    SELECT audits.*
    , DENSE_RANK () OVER (PARTITION BY audits.ticket_id ORDER BY audits.audit_created_at DESC, audits.audit_event_id DESC) AS audit_rank
    FROM audits
    JOIN tickets_sla ON audits.ticket_id = tickets_sla.ticket_id AND audits.audit_created_at <= tickets_sla.audit_created_at
    WHERE audit_field = 'priority'
    AND tickets_sla.audit_rank = 1 --this is the SLA we've applied for the ticket
    AND audit_value != 'null'
    
),

/*Append priority to SLA policy per ticket*/
tickets_sla_priority AS (

    SELECT DISTINCT tickets_sla.*
    , tickets_priority.audit_value AS priority
    FROM tickets_sla
    LEFT JOIN tickets_priority ON tickets_sla.ticket_id = tickets_priority.ticket_id
    WHERE tickets_sla.audit_rank = 1 
    AND tickets_priority.audit_rank = 1

),

ticket_metrics_sla AS (

    SELECT *
    , 
        /* The following is a stopgap solution to set the minimum value between first resolution,
           full resolution and reply time for calendar and business hours respectively.
           In Snowflake, LEAST will always return NULL if any input to the function is NULL.
           In the event all three metrics are NULL, NULL should be returned */
    IFF(
        first_resolution_time_in_minutes_during_calendar_hours IS NULL
        AND full_resolution_time_in_minutes_during_calendar_hours IS NULL
        AND reply_time_in_minutes_during_calendar_hours IS NULL,
            NULL,
                LEAST(
                    COALESCE(first_resolution_time_in_minutes_during_calendar_hours, 50000000),
                    -- 50,000,000 is roughly 100 years, sufficient to not be the LEAST value
                    COALESCE(full_resolution_time_in_minutes_during_calendar_hours, 50000000),
                    COALESCE(reply_time_in_minutes_during_calendar_hours, 50000000)
                    )
        )                                                   AS sla_reply_time_calendar_hours,
    IFF(
        first_resolution_time_in_minutes_during_business_hours IS NULL
        AND full_resolution_time_in_minutes_during_business_hours IS NULL
        AND reply_time_in_minutes_during_business_hours IS NULL,
            NULL,
                LEAST(
                    COALESCE(first_resolution_time_in_minutes_during_business_hours, 50000000),
                    COALESCE(full_resolution_time_in_minutes_during_business_hours, 50000000),
                    COALESCE(reply_time_in_minutes_during_business_hours, 50000000)
                    )
        )                                                   AS sla_reply_time_business_hours
    FROM ticket_metrics

),

reply_timestamps AS (

    SELECT ticket_metrics_sla.ticket_id
    , IFF(
        tickets_sla_priority.sla_policy = 'Emergency Support'  = FALSE,
        ticket_metrics_sla.sla_reply_time_business_hours,
        ticket_metrics_sla.sla_reply_time_calendar_hours
        ) AS first_reply_time_sla
    , TIMEADD(
        minute,
        ticket_metrics_sla.sla_reply_time_calendar_hours,
        ticket_metrics_sla.created_at
        ) AS first_reply_at
    FROM ticket_metrics_sla
    LEFT JOIN tickets_sla_priority ON tickets_sla_priority.ticket_id = ticket_metrics_sla.ticket_id

),

/*For FRT metric, filter on paid users and relevant forms*/
final AS (

    SELECT DISTINCT zendesk_tickets.ticket_id
    , zendesk_tickets.ticket_created_at
    , zendesk_tickets.ticket_form_id
    , date_trunc('month',zendesk_tickets.ticket_created_at)::date AS ticket_create_month
    , reply_timestamps.first_reply_at
    , date_trunc('month',reply_timestamps.first_reply_at)::date AS first_reply_month
    , date_trunc('week',reply_timestamps.first_reply_at)::date AS first_reply_week
    , WEEK(reply_timestamps.first_reply_at) AS first_reply_week_num
    , 'captured in tableau' AS vlookup_column
    , reply_timestamps.first_reply_time_sla --includes business/calendar calculations
    , tickets_sla_priority.sla_policy
    , tickets_sla_priority.priority
    , slas.policy_metrics_target as sla_target
    , IFF(reply_timestamps.first_reply_time_sla <= sla_target, 
      True, False)                                            AS was_sla_achieved
    , IFF(reply_timestamps.first_reply_time_sla > sla_target,
      True, False)                                            AS was_sla_breached
    , ticket_tags
    FROM zendesk_tickets
    LEFT JOIN reply_timestamps ON reply_timestamps.ticket_id = zendesk_tickets.ticket_id
    LEFT JOIN tickets_sla_priority ON tickets_sla_priority.ticket_id = zendesk_tickets.ticket_id
    LEFT JOIN slas ON tickets_sla_priority.priority = slas.policy_metrics_priority AND tickets_sla_priority.sla_policy = slas.zendesk_sla_title
    WHERE (
        zendesk_tickets.ticket_tags LIKE '%''silver''%' OR
        zendesk_tickets.ticket_tags LIKE '%''basic''%' OR
        zendesk_tickets.ticket_tags LIKE '%''starter''%' OR
        zendesk_tickets.ticket_tags LIKE '%''premium''%' OR
        zendesk_tickets.ticket_tags LIKE '%''gold''%' OR
        zendesk_tickets.ticket_tags LIKE '%''bronze''%' OR
        zendesk_tickets.ticket_tags LIKE '%''ultimate''%'
        )
    AND zendesk_tickets.ticket_tags NOT LIKE '%''autoresponder_2fa''%'
    AND zendesk_tickets.ticket_tags NOT LIKE '%''autoclose_nonapproved_users''%'
    AND zendesk_tickets.ticket_tags NOT LIKE '%''submitted_via_gitlab_email''%'
    AND zendesk_tickets.ticket_tags NOT LIKE '%''restricted_account_message''%'
    AND zendesk_tickets.ticket_tags NOT LIKE '%''close_unmonitored_inbox''%'
    AND zendesk_tickets.ticket_form_id IN (360000818199,360000837100,360000803379,334447,426148,4414917877650,360001172559,360001264259)

)

SELECT *
FROM final

{{config({
    "schema": "legacy"
  })
}}

WITH source AS (

    SELECT *
    FROM {{ ref('zendesk_ticket_metrics_source') }}

),

renamed AS (

    SELECT
        *,
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


    FROM source

)

SELECT *
FROM renamed

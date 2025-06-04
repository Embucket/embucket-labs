WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'ticket_metrics') }}

),

renamed AS (

    SELECT

        --ids
        id                                                                                                       AS ticket_metrics_id,
        ticket_id,

        --fields
        COALESCE(agent_wait_time_in_minutes['business'],agent_wait_time_in_minutes__business)::FLOAT             AS agent_wait_time_in_minutes_business_hours,
        COALESCE(agent_wait_time_in_minutes['calendar'],agent_wait_time_in_minutes__calendar)::FLOAT             AS agent_wait_time_in_minutes_calendar_hours,
        COALESCE(first_resolution_time_in_minutes['business'],first_resolution_time_in_minutes__business)::FLOAT AS first_resolution_time_in_minutes_during_business_hours,
        COALESCE(first_resolution_time_in_minutes['calendar'],first_resolution_time_in_minutes__calendar)::FLOAT AS first_resolution_time_in_minutes_during_calendar_hours,
        COALESCE(full_resolution_time_in_minutes['business'],full_resolution_time_in_minutes__business)::FLOAT   AS full_resolution_time_in_minutes_during_business_hours,
        COALESCE(full_resolution_time_in_minutes['calendar'],full_resolution_time_in_minutes__calendar)::FLOAT   AS full_resolution_time_in_minutes_during_calendar_hours,
        COALESCE(on_hold_time_in_minutes['business'],on_hold_time_in_minutes__business)::FLOAT                   AS on_hold_time_in_minutes_during_business_hours,
        COALESCE(on_hold_time_in_minutes['calendar'],on_hold_time_in_minutes__calendar)::FLOAT                   AS on_hold_time_in_minutes_during_calendar_hours,
        reopens,
        replies                                                                                                  AS total_replies,
        COALESCE(reply_time_in_minutes['business'],reply_time_in_minutes__business)::FLOAT                       AS reply_time_in_minutes_during_business_hours,
        COALESCE(reply_time_in_minutes['calendar'],reply_time_in_minutes__calendar)::FLOAT                       AS reply_time_in_minutes_during_calendar_hours,
        COALESCE(requester_wait_time_in_minutes['business'],requester_wait_time_in_minutes__business)::FLOAT     AS requester_wait_time_in_minutes_during_business_hours,
        COALESCE(requester_wait_time_in_minutes['calendar'],requester_wait_time_in_minutes__calendar)::FLOAT     AS requester_wait_time_in_minutes_during_calendar_hours,
        assignee_stations                                                                                        AS assignee_station_number,
        group_stations                                                                                           AS group_station_number,

        --dates
        created_at,
        assigned_at,
        initially_assigned_at,
        latest_comment_added_at,
        solved_at,
        updated_at,
        assignee_updated_at

    FROM source

)

SELECT *
FROM renamed

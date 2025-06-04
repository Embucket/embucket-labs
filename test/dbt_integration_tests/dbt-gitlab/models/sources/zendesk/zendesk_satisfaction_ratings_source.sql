WITH source AS (

    SELECT *
    FROM {{ source('zendesk', 'satisfaction_ratings') }}

),

renamed AS (

    SELECT

        --ids
        id                                                  AS satisfaction_rating_id,
        assignee_id                                         AS assignee_id,
        group_id                                            AS group_id,
        reason_id                                           AS reason_id,
        requester_id                                        AS requester_id,
        ticket_id                                           AS ticket_id,

        --field
        comment                                             AS comment,
        reason                                              AS reason,
        score,
        url                                                 AS satisfaction_rating_url,


        --dates
        created_at,
        updated_at

    FROM source

)

SELECT *
FROM renamed

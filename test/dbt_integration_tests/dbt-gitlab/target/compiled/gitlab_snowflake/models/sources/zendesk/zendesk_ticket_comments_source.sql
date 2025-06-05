

WITH source AS (

    SELECT *
    FROM EMBUCKET.tap_zendesk.ticket_comments

),

renamed AS (

    SELECT

        --ids
        audit_id,
        author_id,
        id                                                  AS ticket_comment_id,
        ticket_id,

        --field
        body                                                AS comment_body,
        html_body                                           AS comment_html_body,
        plain_body                                          AS comment_plain_body,
        public                                              AS is_public,
        "TYPE"                                              AS comment_type,

        --dates
        created_at

    FROM source

)

SELECT *
FROM renamed
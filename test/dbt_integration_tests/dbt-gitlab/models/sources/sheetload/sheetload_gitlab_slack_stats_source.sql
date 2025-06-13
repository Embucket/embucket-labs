WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'gitlab_slack_stats') }}

), renamed AS (

  SELECT
    date::DATE                                             AS entry_date,
    -- running totals
    total_full_members::NUMBER                             AS full_members,
    total_guests::NUMBER                                   AS guests,
    public_channels_in_single_workspace::NUMBER            AS public_channels_in_single_workspace,
    total_enabled_membership::NUMBER                       AS total_membership,
    -- daily totals
    daily_active_members::NUMBER                           AS daily_active_members,
    daily_members_posting_messages::NUMBER                 AS daily_members_posting_messages,
    files_uploaded::NUMBER                                 AS files_uploaded,
    messages_in_dms::NUMBER                                AS messages_in_dms,
    messages_in_private_channels::NUMBER                   AS messages_in_private_channels,
    messages_in_public_channels::NUMBER                    AS messages_in_public_channels,
    messages_in_shared_channels::NUMBER                    AS messages_in_shared_channels,  
    messages_posted::NUMBER                                AS messages_posted,
    messages_posted_by_apps::NUMBER                        AS messages_posted_by_apps,
    messages_posted_by_members::NUMBER                     AS messages_posted_by_members,
    percent_of_messages_in_dms::FLOAT                      AS percent_of_messages_in_dms,
    percent_of_messages_in_private_channels::FLOAT         AS percent_of_messages_in_private_channels,
    percent_of_messages_in_public_channels::FLOAT          AS percent_of_messages_in_public_channels,
    percent_of_views_in_dms::FLOAT                         AS percent_of_views_in_dms,
    percent_of_views_in_private_channels::FLOAT            AS percent_of_views_in_private_channels,
    percent_of_views_in_public_channels::FLOAT             AS percent_of_views_in_public_channels,
    weekly_active_members::NUMBER                          AS weekly_active_members,
    weekly_members_posting_messages::NUMBER                AS weekly_members_posting_messages
  FROM source  

)

SELECT *
FROM renamed

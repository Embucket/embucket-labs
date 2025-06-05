
  
    

create or replace transient table EMBUCKET.gitlab_dotcom.gitlab_dotcom_resource_label_events_source
    

    
    as (WITH source AS (

  SELECT *
  FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_resource_label_events_dedupe_source
  
), renamed AS (

    SELECT
      id                                             AS resource_label_event_id,
      action::NUMBER                                AS action_type_id,
      CASE  WHEN action::NUMBER = 1 THEN 'added'
        WHEN action::NUMBER = 2 THEN 'removed'
  END     AS action_type,
      issue_id::NUMBER                              AS issue_id,
      merge_request_id::NUMBER                      AS merge_request_id,
      epic_id::NUMBER                               AS epic_id,
      label_id::NUMBER                              AS label_id,
      user_id::NUMBER                               AS user_id,
      created_at::TIMESTAMP                          AS created_at,
      cached_markdown_version::VARCHAR               AS cached_markdown_version,
      reference::VARCHAR                             AS referrence,
      reference_html::VARCHAR                        AS reference_html  
    FROM source

)

SELECT *
FROM renamed
    )
;


  
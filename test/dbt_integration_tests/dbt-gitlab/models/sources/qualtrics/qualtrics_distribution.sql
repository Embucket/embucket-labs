WITH source AS (

    SELECT *
    FROM {{ source('qualtrics', 'distribution') }}

), intermediate AS (

    SELECT 
      d.value AS data_by_row,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), outer => true) d

), parsed AS (

    SELECT 
      data_by_row['recipients']['mailingListId']::VARCHAR   AS mailing_list_id,
      data_by_row['id']::VARCHAR                            AS distribution_id,
      data_by_row['surveyLink']['surveyId']::VARCHAR        AS survey_id,
      data_by_row['sendDate']::TIMESTAMP                    AS mailing_sent_at,
      data_by_row['stats']['blocked']::NUMBER              AS email_blocked_count,
      data_by_row['stats']['bounced']::NUMBER              AS email_bounced_count,
      data_by_row['stats']['complaints']::NUMBER           AS complaint_count,
      data_by_row['stats']['failed']::NUMBER               AS email_failed_count,
      data_by_row['stats']['finished']::NUMBER             AS survey_finished_count,
      data_by_row['stats']['opened']::NUMBER               AS email_opened_count,
      data_by_row['stats']['sent']::NUMBER                 AS email_sent_count,
      data_by_row['stats']['skipped']::NUMBER              AS email_skipped_count,
      data_by_row['stats']['started']::NUMBER              AS survey_started_count,
      uploaded_at::TIMESTAMP                                AS uploaded_at
    FROM intermediate

)

SELECT * 
FROM parsed

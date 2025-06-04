WITH source AS (
  SELECT *
  FROM {{ source('gong','call') }}
),

renamed AS (
  SELECT
    id::NUMBER                              AS call_id,
    url::STRING                             AS url,
    media::VARIANT                          AS media,
    custom_data::VARIANT                    AS custom_data,
    sdr_disposition::STRING                 AS sdr_disposition,
    is_private::BOOLEAN                     AS is_private,
    client_unique_id::STRING                AS client_unique_id,
    direction::STRING                       AS direction,
    languages::VARIANT                      AS languages,
    scope::BOOLEAN                          AS scope,
    duration::NUMBER                        AS duration_seconds,
    title::STRING                           AS title,
    meeting_url::STRING                     AS meeting_url,
    systems::VARIANT                        AS systems,
    purpose::STRING                         AS purpose,
    scheduled::TIMESTAMP                    AS scheduled_at,
    media_video_url::STRING                 AS media_video_url,
    company_question_count::NUMBER          AS company_question_count,
    non_company_question_count::NUMBER      AS non_company_question_count,
    started::TIMESTAMP                      AS started_at,
    media_audio_url::STRING                 AS media_audio_url,
    _fivetran_deleted::BOOLEAN              AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP             AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed
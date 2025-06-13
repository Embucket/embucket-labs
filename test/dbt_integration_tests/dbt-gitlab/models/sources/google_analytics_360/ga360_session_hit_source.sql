WITH source AS (

    SELECT *
    FROM {{ source('google_analytics_360', 'session_hit') }}

), renamed AS(

    SELECT
      --Keys
      visit_id::FLOAT                                AS visit_id,
      visitor_id::VARCHAR                            AS visitor_id,

      --Info
      visit_start_time::TIMESTAMP_TZ                 AS visit_start_time,
      visit_start_time::DATE                         AS session_date, 
      hit_number::NUMBER                             AS hit_number,
      DATEADD('millisecond', time, visit_start_time) AS hit_at,
      is_entrance::BOOLEAN                           AS is_entrance,
      is_exit::BOOLEAN                               AS is_exit,
      referer::VARCHAR                               AS referer,
      type::VARCHAR                                  AS hit_type,
      data_source::VARCHAR                           AS data_source,
      page_hostname::VARCHAR                         AS host_name,
      page_page_path::VARCHAR                        AS page_path,
      page_page_title::VARCHAR                       AS page_title,
      event_info_category::VARCHAR                   AS event_category,
      event_info_action::VARCHAR                     AS event_action,
      event_info_label::VARCHAR                      AS event_label

    FROM source

)

SELECT *
FROM renamed

WITH source AS (

    SELECT *
    FROM {{ source('tableau_fivetran', 'extract_refresh_task') }}

), renamed AS (

    SELECT
        id::VARCHAR                                           AS schedule_id,
        schedule_frequency_details_interval_weekday::VARIANT  AS schedule_frequency_details_interval_weekday,
        datasource_id::VARCHAR                                AS datasource_id,
        workbook_id::VARCHAR                                  AS workbook_id,
        schedule_next_run_at::TIMESTAMP                       AS schedule_next_run_at,
        schedule_frequency::VARCHAR                           AS schedule_frequency,
        priority::NUMBER                                      AS priority,
        type::VARCHAR                                         AS schedule_type,
        schedule_frequency_details_start::VARCHAR             AS schedule_frequency_details_start,
        consecutive_failed_count::NUMBER                      AS consecutive_failed_count

    FROM source
    WHERE NOT _fivetran_deleted

)

SELECT *
FROM renamed
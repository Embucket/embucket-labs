WITH gdpr_delete_logs AS (

    SELECT *
    FROM {{ ref('dbt_gdpr_logs_source') }}
    WHERE info_msg != 'Using snowflake connection "macro_gdpr_bulk_delete"'

), max_time_stamps AS (

    -- Retrieve max timestamp by invocationID
    SELECT invocation_id, MAX(time_stamp) max_time_stamp
    FROM gdpr_delete_logs
    GROUP BY 1

), partition_times as (

    -- Use specific log line to retrieve start of e-mail processing,
    -- Then partition by emails,
    -- Max times retrieved above to partition for the last email in the batch.
    SELECT
      logs.time_stamp,
      logs.info_msg,
      logs.invocation_id,
      IFNULL(LEAD(logs.time_stamp) IGNORE NULLS OVER (PARTITION BY logs.invocation_id ORDER BY logs.time_stamp), max_time_stamps.max_time_stamp) AS next_time_stamp
    FROM gdpr_delete_logs logs
    JOIN max_time_stamps ON max_time_stamps.invocation_id=logs.invocation_id
    WHERE logs.info_msg like '%SET email_sha%'

), final AS (

    -- Cleans up the log line line and joins by partition times to apply the e-mail SHA to all relevant rows
    SELECT
      logs.time_stamp,
      logs.info_msg,
      RTRIM(REPLACE(REPLACE(REPLACE(partition_times.info_msg, '        SET email_sha = ''', ''), ''';', ''), '\n', '')) AS email_sha,
      logs.invocation_id
    FROM gdpr_delete_logs logs
    JOIN partition_times
        ON partition_times.invocation_id = logs.invocation_id
        AND logs.time_stamp >= partition_times.time_stamp AND logs.time_stamp < next_time_stamp

)

SELECT *
FROM final
ORDER by 1



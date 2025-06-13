{%- macro scd_type_2(primary_key_renamed, primary_key_raw, source_cte='distinct_source', casted_cte='renamed') -%}

, max_by_primary_key AS (
  
    SELECT
      {{ primary_key_raw }} AS primary_key,
      MAX(
        IFF(max_task_instance IN ( 
              SELECT MAX(max_task_instance) 
              FROM {{ source_cte }} 
            ), 1, 0)
      )                     AS is_in_most_recent_task,
      MAX(max_uploaded_at)  AS max_timestamp
    FROM {{ source_cte }}
    GROUP BY 1

)

, windowed AS (
  
    SELECT
      {{casted_cte}}.*,

      COALESCE( -- First, look for the row immediately following by PK and subtract one millisecond from its timestamp.
        DATEADD('millisecond', -1, LEAD(valid_from) OVER (
          PARTITION BY {{casted_cte}}.{{primary_key_renamed}}
          ORDER BY valid_from)
        ),
        -- If row has no following rows, check when it's valid until (NULL if it appeared in latest task instance.)
          IFF(is_in_most_recent_task = FALSE, max_by_primary_key.max_timestamp, NULL)
      )                                  AS valid_to,
      IFF(valid_to IS NULL, True, False) AS is_currently_valid

    FROM {{casted_cte}}
    LEFT JOIN max_by_primary_key
      ON {{casted_cte}}.{{primary_key_renamed}} = max_by_primary_key.primary_key
    ORDER BY valid_from, valid_to

)

SELECT *
FROM windowed

{%- endmacro -%}

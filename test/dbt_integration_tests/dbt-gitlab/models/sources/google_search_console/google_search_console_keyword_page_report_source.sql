WITH source AS (
  
   SELECT *
   FROM {{ source('google_search_console','keyword_page_report') }}
 
), final AS (
 
    SELECT   
      country::TEXT                    AS country,
      date::DATE                       AS date,
      device::TEXT                     AS device,
      page::TEXT                       AS page,
      query::TEXT                      AS query,
      search_type::TEXT                AS search_type,
      site::TEXT                       AS site,
      clicks::FLOAT                    AS clicks,
      impressions::FLOAT               AS impressions,
      ctr::FLOAT                       AS ctr,
      position::FLOAT                  AS position,
      _fivetran_synced::TIMESTAMP_TZ   AS _fivetran_synced
    FROM source
)

SELECT *
FROM final

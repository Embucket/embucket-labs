WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_zoom_webinar_history_source') }}
  WHERE is_deleted = FALSE

)

SELECT *
FROM source

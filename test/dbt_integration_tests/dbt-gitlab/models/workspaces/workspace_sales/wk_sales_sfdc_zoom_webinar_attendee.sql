WITH source AS (

  SELECT *
  FROM {{ ref('sfdc_zoom_webinar_attendee_source') }}
  WHERE is_deleted = FALSE

)

SELECT *
FROM source

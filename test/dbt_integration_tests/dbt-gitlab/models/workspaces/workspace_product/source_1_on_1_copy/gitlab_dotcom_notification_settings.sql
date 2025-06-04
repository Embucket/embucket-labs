WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_notification_settings_source') }}

)

SELECT *
FROM source

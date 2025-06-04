WITH source AS (
  SELECT
    email,
    optedout  AS opted_out,
    updatedat AS updated_at

  FROM
    {{ source('rally_webhook_stitch', 'data') }}
)

SELECT *
FROM source

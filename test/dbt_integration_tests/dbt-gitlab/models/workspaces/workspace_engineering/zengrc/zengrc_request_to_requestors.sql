WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_request_source') }}

)

SELECT
  source.request_id,
  requestors.value['id']::NUMBER     AS requestor_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.requestors)) requestors
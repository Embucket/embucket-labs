WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_request_source') }}

)

SELECT
  source.request_id,
  mapped_controls.value['id']::NUMBER     AS control_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_controls)) mapped_controls
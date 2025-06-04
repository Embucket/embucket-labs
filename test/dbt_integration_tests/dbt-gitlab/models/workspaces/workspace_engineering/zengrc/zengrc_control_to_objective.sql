WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_control_source') }}

)

SELECT
  source.control_id,
  mapped_objective.value['id']::NUMBER     AS objective_id
FROM source
INNER JOIN LATERAL FLATTEN(INPUT => TRY_PARSE_JSON(source.mapped_objectives)) mapped_objective
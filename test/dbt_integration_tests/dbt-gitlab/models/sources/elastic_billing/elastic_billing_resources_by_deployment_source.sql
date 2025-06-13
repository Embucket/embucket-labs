WITH source AS (
  SELECT *
  FROM {{ source('elastic_billing', 'itemized_costs_by_deployment') }}
),

parsed AS (
  SELECT
    source.deployment_id,
    source.extraction_start_date,
    source.extraction_end_date,
    resources.value['period']['start']::TIMESTAMP AS resource_start_date,
    resources.value['period']['end']::TIMESTAMP   AS resource_end_date,
    resources.value['hours']::FLOAT               AS hours,
    resources.value['instance_count']::NUMBER     AS instance_count,
    resources.value['kind']::VARCHAR              AS kind,
    resources.value['price']::VARCHAR             AS cost,
    resources.value['name']::VARCHAR              AS resource_name,
    resources.value['price_per_hour']::FLOAT      AS price_per_hour,
    resources.value['sku']::VARCHAR               AS sku,
    TO_TIMESTAMP(source._uploaded_at::INT)        AS _uploaded_at
  FROM source
  INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(payload), outer => TRUE) AS dims
  INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(dims.value), outer => TRUE, mode => 'ARRAY') AS resources
  WHERE dims.path = 'resources'
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY source.extraction_end_date, source.deployment_id, resources.value['sku']::VARCHAR
      ORDER BY TO_TIMESTAMP(source._uploaded_at::INT) DESC
    ) = 1
)

SELECT *
FROM parsed

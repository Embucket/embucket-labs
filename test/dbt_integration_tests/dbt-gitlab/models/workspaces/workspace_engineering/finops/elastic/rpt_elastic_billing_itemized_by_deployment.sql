WITH data_transfer_source AS (

  SELECT *
  FROM {{ ref ('elastic_billing_data_transfer_by_deployment_source') }}

),

resource_source AS (

  SELECT *
  FROM {{ ref ('elastic_billing_resources_by_deployment_source') }}

),

renamed AS (

  SELECT
    deployment_id,
    extraction_start_date,
    extraction_end_date,
    'data_transfer'                      AS charge_type,
    sku,
    cost,
    resource_quantity_value              AS usage,
    CASE WHEN formated_quantity_value LIKE '%GB%' THEN 'bytes'
      WHEN formated_quantity_value LIKE '%requests%' THEN 'requests'
    END                                  AS usage_unit,
    ARRAY_CONSTRUCT(type, resource_name) AS details
  FROM data_transfer_source
  UNION ALL
  SELECT
    deployment_id,
    extraction_start_date,
    extraction_end_date,
    'capacity' AS charge_type,
    sku,
    cost,
    hours      AS usage,
    'hours'    AS usage_unit,
    ARRAY_CONSTRUCT(resource_start_date, resource_end_date, instance_count, price_per_hour, resource_name)
  FROM resource_source
),

transformed AS (
  SELECT
    extraction_end_date AS day,
    deployment_id,
    charge_type,
    sku,
    usage_unit,
    CASE WHEN day = DATE_TRUNC('month', DAY) THEN cost ELSE -- handle first day of the month where cost and usage would be null without case statement
        cost - LAG(cost, 1) OVER (PARTITION BY
          DATE_TRUNC('month', extraction_end_date),
          deployment_id,
          charge_type,
          sku,
          usage_unit
        ORDER BY extraction_end_date)
    END                 AS daily_cost,
    CASE WHEN day = DATE_TRUNC('month', DAY) THEN usage ELSE -- handle first day of the month where cost and usage would be null without case statement
        usage - LAG(usage, 1) OVER (PARTITION BY
          DATE_TRUNC('month', extraction_end_date),
          deployment_id,
          charge_type,
          sku,
          usage_unit
        ORDER BY extraction_end_date)
    END                 AS daily_usage,
    cost,
    usage,
    details
  FROM renamed
)

SELECT
  day,
  deployment_id,
  charge_type,
  sku,
  usage_unit,
  daily_cost,
  cost  AS monthly_accumulated_cost,
  daily_usage,
  usage AS monthly_accumulated_usage,
  details
FROM transformed


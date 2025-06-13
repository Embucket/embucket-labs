WITH source AS (
  SELECT *
  FROM {{ source('elastic_billing', 'itemized_costs') }}
),

parsed AS (
  SELECT
    source.extraction_start_date,
    source.extraction_end_date,
    CASE resources_and_transfer.value['sku']::VARCHAR
      WHEN 'gcp.data-transfer-in' THEN 'gcp_data_transfer_in'
      WHEN 'gcp.data-transfer-inter-node' THEN 'gcp_data_transfer_inter_node'
      WHEN 'gcp.data-transfer-out' THEN 'gcp_data_transfer_out'
      WHEN 'gcp.snapshot-api-1k' THEN 'gcp_snapshot_storage_api'
      WHEN 'gcp.snapshot-storage' THEN 'gcp_snapshot_storage'
    END                                                                  AS charge_type,
    resources_and_transfer.value['cost']::FLOAT                          AS cost,
    resources_and_transfer.value['name']::VARCHAR                        AS resource_name,
    resources_and_transfer.value['quantity']['value']::FLOAT             AS resource_quantity_value,
    resources_and_transfer.value['quantity']['formatted_value']::VARCHAR AS formated_quantity_value,
    resources_and_transfer.value['rate']['value']::FLOAT                 AS rate_value,
    resources_and_transfer.value['rate']['formatted_value']::VARCHAR     AS formated_rate_value,
    resources_and_transfer.value['sku']::VARCHAR                         AS sku,
    resources_and_transfer.value['type']::VARCHAR                        AS type,
    TO_TIMESTAMP(source._uploaded_at::INT)                               AS _uploaded_at
  FROM source
  INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(payload), outer => TRUE) AS dims
  INNER JOIN LATERAL FLATTEN(input => PARSE_JSON(dims.value), outer => TRUE, mode => 'ARRAY') AS resources_and_transfer
  WHERE dims.path = 'data_transfer_and_storage'
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY source.extraction_end_date, resources_and_transfer.value['sku']::VARCHAR
      ORDER BY TO_TIMESTAMP(source._uploaded_at::INT) DESC
    ) = 1
)

SELECT *
FROM parsed

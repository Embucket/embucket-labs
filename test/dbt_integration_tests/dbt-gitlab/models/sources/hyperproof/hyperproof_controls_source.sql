WITH source AS (
  
    SELECT * 
    FROM {{ source('hyperproof', 'controls') }}
    
), metric_per_row AS (

    SELECT 
      data_by_row.value['_isPartialData']::VARCHAR                        AS is_partial_data,
      data_by_row.value['controlIdentifier']::VARCHAR                     AS control_identifier,
      data_by_row.value['controlType']::VARCHAR                           AS control_type,
      data_by_row.value['createdBy']::VARCHAR                             AS created_by,
      data_by_row.value['description']::VARCHAR                           AS description,
      data_by_row.value['domainId']::VARCHAR                              AS domain_id,
      data_by_row.value['domainName']::VARCHAR                            AS domain_name,
      data_by_row.value['id']::VARCHAR                                    AS control_id,
      data_by_row.value['name']::VARCHAR                                  AS name,
      data_by_row.value['orgId']::VARCHAR                                 AS org_id,
      data_by_row.value['overrideHealth']::VARCHAR                        AS override_health,
      data_by_row.value['owner']::VARCHAR                                 AS owner,
      data_by_row.value['permissions']::VARCHAR                           AS permissions,
      data_by_row.value['status']::VARCHAR                                AS status,
      data_by_row.value['updatedBy']::VARCHAR                             AS updatedBy,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row

)

SELECT *
FROM metric_per_row
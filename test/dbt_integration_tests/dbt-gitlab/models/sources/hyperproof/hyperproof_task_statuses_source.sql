WITH source AS (
  
    SELECT * 
    FROM {{ source('hyperproof', 'taskstatuses') }}
    
),  metric_per_row AS (

    SELECT
        data_by_row.value['color']::VARCHAR              AS color,
        data_by_row.value['createdBy']::VARCHAR          AS created_by,
        data_by_row.value['createdOn']::TIMESTAMP        AS created_on,
        data_by_row.value['icon']::VARCHAR               AS icon,
        data_by_row.value['id']::VARCHAR                 AS task_status_id,
        data_by_row.value['name']::VARCHAR               AS task_status_name,
        data_by_row.value['orgId']::VARCHAR              AS org_id,
        data_by_row.value['sortOrder']::VARCHAR          AS sort_order,
        data_by_row.value['status']::VARCHAR             AS status,
        data_by_row.value['type']::VARCHAR               AS type,
        data_by_row.value['updatedBy']::VARCHAR          AS updated_by,
        data_by_row.value['updatedOn']::TIMESTAMP        AS updated_on,
        uploaded_at

    FROM source,
    LATERAL FLATTEN(input => PARSE_JSON(jsontext), OUTER => True) data_by_row

)
SELECT *
FROM metric_per_row
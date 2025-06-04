WITH source AS (

    SELECT *
    FROM {{ source('thanos', 'periodic_queries') }}

), parsed AS (

    SELECT 
        pq_2.value['metric']['backend']::VARCHAR                     AS metric_backend,
        PARSE_JSON(pq_2.value['value'][0]::FLOAT)::NUMBER::TIMESTAMP AS metric_created_at,
        NULLIF(pq_2.value['value'][1], 'NaN')::FLOAT                 AS metric_value,
        pq_1.value['data']['resultType']::VARCHAR                    AS result_type,
        pq_1.value['status']:: VARCHAR                               AS status_type,
        pq_1.this['message']:: VARCHAR                               AS message_type,
        pq_1.this['status_code']:: NUMBER                            AS status_code,
        pq_1.this['success']:: BOOLEAN                               AS is_success
    FROM source pq,
        lateral flatten(input => pq.jsontext['total_haproxy_bytes_out']) pq_1,
        lateral flatten(input => pq.jsontext['total_haproxy_bytes_out']['body']['data']['result'],outer => true) pq_2
    WHERE result_type IS NOT NULL AND status_type IS NOT NULL

) 

SELECT * 
FROM parsed

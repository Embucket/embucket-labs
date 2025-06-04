WITH source AS (

    SELECT *
    FROM {{ source('demandbase', 'account_site_page_metrics') }}

), renamed AS (

    SELECT
      jsontext['account_id']::NUMBER                AS account_id,
      jsontext['base_page']::VARCHAR                AS base_page,
      jsontext['date']::DATE                        AS metric_date,
      jsontext['page_view_count']::NUMBER           AS page_view_count,
      jsontext['partition_date']::DATE              AS partition_date
    FROM source
    WHERE partition_date = (
        SELECT MAX(jsontext['partition_date']::DATE) 
        FROM source
    )

)

SELECT *
FROM renamed